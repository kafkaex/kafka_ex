defmodule KafkaEx.Client.MetadataStore.Round do
  @moduledoc false

  require Logger

  alias KafkaEx.Client
  alias KafkaEx.Client.ConnectionStateMachine
  alias KafkaEx.Client.MetadataLog
  alias KafkaEx.Client.RequestBuilder
  alias KafkaEx.Client.ResponseParser
  alias KafkaEx.Client.State
  alias KafkaEx.Cluster.Broker
  alias KafkaEx.Cluster.ClusterMetadata
  alias KafkaEx.Config
  alias KafkaEx.Messages.FindCoordinator
  alias KafkaEx.Support.Retry
  alias KafkaEx.Telemetry

  @protocol Application.compile_env(:kafka_ex, :protocol, KafkaEx.Protocol.KayrockProtocol)

  @api_versions_max_retries 3
  @api_versions_retry_base_delay_ms 100
  @ladder_margin 500
  @no_bytes %{bytes_sent: 0, bytes_received: 0}
  @unsupported_logged {__MODULE__, :unsupported_logged}

  @type address :: {binary(), :inet.port_number()}
  @type kind :: :load | :metadata | :find_coordinator
  @type spec :: %{
          required(:kind) => kind(),
          required(:candidates) => [{address(), pid()}],
          required(:brokers) => %{optional(address()) => Broker.t()},
          required(:request_opts) => Keyword.t(),
          required(:known_missing) => MapSet.t(),
          required(:ladder_delay) => non_neg_integer(),
          required(:connect_timeout) => pos_integer(),
          required(:connect_slack) => non_neg_integer(),
          required(:request_timeout) => pos_integer(),
          required(:request_slack) => non_neg_integer(),
          required(:deadline) => integer()
        }
  @type budget :: %{
          required(:connect_slack) => non_neg_integer(),
          required(:request_slack) => non_neg_integer(),
          required(:kind) => kind(),
          required(:candidates) => list(),
          required(:ladder_delay) => non_neg_integer(),
          required(:connect_timeout) => pos_integer(),
          required(:request_timeout) => pos_integer(),
          optional(atom()) => term()
        }
  @type result ::
          {:ok,
           %{
             metadata: ClusterMetadata.t(),
             unknown: [binary()],
             missing: [binary()],
             address: address(),
             versions: map()
           }}
          | {:ok, %{coordinator: FindCoordinator.t() | {:error, term()}, address: address()}}
          | {:error, term()}
          | {:raised, {:error, Exception.t(), Exception.stacktrace()}}

  @doc false
  @spec api_versions_max_retries() :: pos_integer()
  def api_versions_max_retries, do: @api_versions_max_retries

  @spec bound(budget()) :: pos_integer()
  def bound(spec), do: walk_cost(spec) + (Client.retry_count() - 1) * spec.ladder_delay + 1_000

  defp walk_cost(spec) do
    per_candidate = spec.connect_timeout + spec.connect_slack + spec.request_timeout + spec.request_slack
    length(spec.candidates) * (per_candidate + api_versions_retries(spec))
  end

  defp api_versions_retries(%{kind: :load} = spec) do
    sleeps = Enum.sum(for attempt <- 0..(@api_versions_max_retries - 1), do: backoff(attempt))
    @api_versions_max_retries * (spec.connect_timeout + spec.connect_slack) + sleeps
  end

  defp api_versions_retries(_spec), do: 0

  @spec run(spec()) :: result()
  def run(spec) do
    Process.delete(@unsupported_logged)
    if spec.kind == :find_coordinator, do: coordinator(spec), else: metadata(spec, Client.retry_count())
  rescue
    exception -> {:raised, {:error, exception, __STACKTRACE__}}
  end

  defp coordinator(spec) do
    case first_response(spec, &build_find_coordinator(&1, spec)) do
      {:ok, response, address, _versions} -> {:ok, %{coordinator: parse_coordinator(response, spec), address: address}}
      {:error, {:build, _at, reason}} -> {:error, reason}
      {:error, reason} -> {:error, reason}
    end
  end

  defp metadata(spec, attempts_left) do
    started = now()

    case first_response(spec, &build_metadata(&1, spec)) do
      {:ok, response, address, versions} ->
        parsed(spec, response, address, versions, attempts_left, now() - started)

      {:error, {:connected, address, versions}} ->
        bootstrap_only(spec, address, versions)

      {:error, {tag, {address, versions}, _reason}} when tag in [:build, :refused] and spec.kind == :load ->
        bootstrap_only(spec, address, versions)

      {:error, {:build, _at, reason}} ->
        {:error, reason}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp parsed(spec, response, address, versions, attempts_left, walked) do
    case ResponseParser.metadata_response(response) do
      {:ok, metadata} ->
        missing = MetadataLog.missing_topics(Keyword.get(spec.request_opts, :topics, []), metadata)

        if ladder?(spec, missing, attempts_left, walked) do
          Process.sleep(spec.ladder_delay)
          metadata(spec, attempts_left - 1)
        else
          {:ok, %{metadata: metadata, unknown: [], missing: missing, address: address, versions: versions}}
        end

      {:error, _reason} ->
        Logger.error("Unable to parse metadata response from brokers.")
        {:error, :parse_error}
    end
  end

  # Another attempt at the walk time just seen must fit in the round's bound; only a far slower walk meets the backstop.
  defp ladder?(spec, missing, attempts_left, walked) do
    attempts_left > 1 and Enum.any?(missing, &(not MapSet.member?(spec.known_missing, &1))) and
      now() + spec.ladder_delay + min(walked, walk_cost(spec)) + @ladder_margin <= spec.deadline
  end

  # A boot whose Metadata failed after a connect still boots, with the bootstrap brokers keyed by index.
  defp bootstrap_only(spec, address, versions) do
    brokers =
      spec.candidates
      |> Enum.with_index(1)
      |> Map.new(fn {{{host, port}, _pid}, index} -> {index, %Broker{host: host, port: port}} end)

    metadata = %ClusterMetadata{brokers: brokers}
    {:ok, %{metadata: metadata, unknown: [], missing: [], address: address, versions: versions}}
  end

  defp first_response(spec, build) do
    spec.candidates
    |> walk(spec, build, %{acc: {:error, :no_broker}, logged?: false, span: nil})
    |> all_failed(spec)
  end

  defp walk([], _spec, _build, %{span: nil, acc: acc}), do: acc

  defp walk([], _spec, _build, %{span: span, acc: acc}) do
    stop_span(%{span | meta: %{span.meta | bytes_received: 0}}, {:error, :broker_failed}, %{})
    acc
  end

  defp walk([{address, pid} | rest], spec, build, progress) do
    case connect(spec, address, pid, @api_versions_max_retries) do
      {:connect_error, reason} ->
        walk(rest, spec, build, %{progress | acc: not_connected(progress.acc, reason)})

      {:ok, versions} ->
        case build.(versions) do
          {:ok, request} ->
            await(spec, build, {address, pid, versions, request}, rest, connected(progress, address, versions))

          {:error, reason} ->
            walk(rest, spec, build, build_failed(progress, spec, {address, versions}, reason))
        end
    end
  end

  defp connected(%{acc: {:error, {:connected, _, _}}} = progress, _address, _versions), do: progress
  defp connected(%{acc: {:error, {:refused, _, _}}} = progress, _address, _versions), do: progress
  defp connected(progress, address, versions), do: %{progress | acc: {:error, {:connected, address, versions}}}

  # One build-failure line per walk, as the client logged one per refresh.
  defp build_failed(progress, spec, at, reason) do
    if not progress.logged?, do: log_build_failure(spec, reason)

    case progress.acc do
      {:error, {tag, _at, _reason}} when tag in [:build, :connected, :refused] -> %{progress | logged?: true}
      _acc -> %{progress | acc: {:error, {:build, at, reason}}, logged?: true}
    end
  end

  # One span per walk, opened once a connection is up for the write: a walk that writes nothing has none.
  defp await(spec, build, {address, pid, versions, request}, rest, progress) do
    case negotiated(spec, build, pid, versions, dispatch(spec, request(request), address)) do
      {:ok, dispatch, versions} when progress.span == nil ->
        progress = open_span(%{progress | acc: {:error, {:connected, address, versions}}}, dispatch.request)
        opened(progress, &write(spec, build, {address, pid, versions, dispatch}, rest, &1))

      {:ok, dispatch, versions} ->
        write(spec, build, {address, pid, versions, dispatch}, rest, restamp(progress, dispatch.request))

      {:refused, reason, versions} ->
        log_failed(spec, address, reason)
        walk(rest, spec, build, refused(progress, {address, versions}, reason))

      {:error, reason, _meta} ->
        not_written(spec, address, reason)
        walk(rest, spec, build, progress)
    end
  end

  # A refused request was never written, so rebuilding it at the connection's versions costs no attempt.
  defp negotiated(spec, build, pid, versions, dispatch) do
    case await_connection(spec, pid, dispatch) do
      :ok -> {:ok, dispatch, versions}
      {:error, {:unsupported_version, versions}, _meta} -> rebuild(spec, build, pid, versions, dispatch)
      error -> error
    end
  end

  # The same dispatch keeps its deadline; a pinned version outside the connection's range is never downgraded.
  defp rebuild(spec, build, pid, versions, dispatch) do
    with {:ok, request} <- build.(versions),
         rebuilt = carrying(dispatch, request(request)),
         :ok <- await_connection(spec, pid, rebuilt) do
      {:ok, rebuilt, versions}
    else
      {:error, {:unsupported_version, versions}, _meta} -> {:refused, :api_version_no_supported, versions}
      {:error, reason} -> {:refused, reason, versions}
      error -> error
    end
  end

  defp await_connection(spec, pid, dispatch) do
    call_connection(
      fn -> ConnectionStateMachine.await(pid, dispatch, spec.request_timeout + spec.request_slack) end,
      :not_connected
    )
  end

  # A walk that writes nothing ends on the refusal, not on a missing broker.
  defp refused(%{span: nil} = progress, at, reason), do: %{progress | acc: {:error, {:refused, at, reason}}}
  defp refused(progress, _at, _reason), do: progress

  # The rest of the walk runs under the open span, so a crash in it still ends the span.
  defp opened(progress, rest_of_walk) do
    rest_of_walk.(progress)
  catch
    kind, reason ->
      Telemetry.exception_span(progress.span.handle, progress.span.metadata, kind, reason, __STACKTRACE__)
      :erlang.raise(kind, reason, __STACKTRACE__)
  end

  # Same dispatch, so the same deadline: the write never gets a second request timeout.
  defp write(spec, build, {address, pid, versions, dispatch}, rest, progress) do
    left = max(dispatch.deadline + spec.request_slack - now(), 0)

    # The connection was up at the await: a drop since then is a dead socket's send.
    case call_connection(fn -> ConnectionStateMachine.write(pid, dispatch, left) end, :closed) do
      {:ok, frame, meta} ->
        case ResponseParser.deserialize(frame, dispatch.request) do
          {:ok, response} ->
            stop_span(%{progress.span | meta: meta}, {:ok, response}, broker_info(spec, address))
            {:ok, response, address, versions}

          {:error, :parse_error} ->
            walk(rest, spec, build, put_meta(progress, meta))
        end

      # Encoding is deterministic: no other candidate can do better.
      {:error, {:exception, kind, reason, stacktrace}, _meta} ->
        Logger.warning("Network call to #{describe(spec, address)} failed: #{inspect({:exception, kind, reason})}")
        Telemetry.exception_span(progress.span.handle, progress.span.metadata, kind, reason, stacktrace)
        progress.acc

      {:error, reason, meta} ->
        log_failed(spec, address, reason)
        walk(rest, spec, build, put_meta(progress, meta))
    end
  end

  defp open_span(%{span: nil} = progress, request) do
    metadata = Telemetry.request_metadata(request, %{})
    %{progress | span: %{handle: Telemetry.start_span([:kafka_ex, :request], metadata), metadata: metadata, meta: nil}}
  end

  # A later candidate keeps the walk's span; its stop reports the version this candidate writes.
  defp restamp(progress, request) do
    %{progress | span: %{progress.span | metadata: Telemetry.request_metadata(request, %{})}}
  end

  defp put_meta(progress, meta), do: %{progress | span: %{progress.span | meta: meta}}

  defp stop_span(%{handle: handle, metadata: metadata, meta: meta}, result, broker) do
    Telemetry.stop_span(handle, {result, Map.merge(metadata, Map.put(meta, :broker, broker))})
  end

  defp not_connected({:error, {tag, _at, _detail}} = acc, _reason) when tag in [:build, :connected, :refused], do: acc
  defp not_connected({:error, {:api_versions, _}} = acc, _reason), do: acc
  defp not_connected({:error, _}, reason), do: {:error, reason}

  defp all_failed({:ok, _response, _address, _versions} = ok, _spec), do: ok
  defp all_failed({:error, {:build, _, _}} = error, _spec), do: error

  defp all_failed({:error, reason} = error, %{kind: :load} = spec) do
    log_all_failed(spec, reason)
    error
  end

  defp all_failed({:error, {:connected, _, _}}, spec), do: log_all_failed(spec, :broker_failed)
  defp all_failed({:error, {:refused, _at, reason}}, spec), do: log_all_failed(spec, reason)
  defp all_failed({:error, _reason}, spec), do: log_all_failed(spec, :no_broker)

  defp log_all_failed(%{kind: :find_coordinator} = spec, reason) do
    log_coordinator_failure(spec, reason)
    {:error, reason}
  end

  defp log_all_failed(spec, reason) do
    Logger.error("Unable to fetch metadata from any brokers. Timeout is #{spec.request_timeout}.")
    {:error, reason}
  end

  defp connect(spec, address, pid, retries_left) do
    case call(fn -> ConnectionStateMachine.connect(pid, spec.connect_timeout + spec.connect_slack) end) do
      {:ok, versions} ->
        {:ok, versions}

      {:error, {:api_versions, reason}} when spec.kind == :load ->
        retry_api_versions(spec, address, pid, reason, retries_left)

      {:error, reason} ->
        Logger.debug("#{describe(spec, address)} not connected, skipping")
        {:connect_error, reason}
    end
  end

  defp retry_api_versions(spec, address, pid, reason, retries_left) do
    cond do
      retries_left <= 0 ->
        Logger.error("ApiVersions negotiation failed after #{@api_versions_max_retries} attempts: #{inspect(reason)}")
        {:connect_error, {:api_versions, reason}}

      Retry.transient_error?(reason) ->
        delay = backoff(@api_versions_max_retries - retries_left)

        Logger.warning(
          "ApiVersions negotiation failed with #{inspect(reason)}, " <>
            "retrying in #{delay}ms (#{retries_left - 1} retries left)"
        )

        Process.sleep(delay)
        connect(spec, address, pid, retries_left - 1)

      true ->
        Logger.error("ApiVersions negotiation failed with non-retryable error: #{inspect(reason)}")
        {:connect_error, {:api_versions, reason}}
    end
  end

  defp backoff(attempt), do: Retry.backoff_delay(attempt, @api_versions_retry_base_delay_ms)

  defp request(request), do: %{request | client_id: Config.client_id(), correlation_id: nil}

  defp dispatch(spec, request, address) do
    %{
      node_id: broker_info(spec, address).node_id,
      request: request,
      info: @protocol.request_info(request),
      reply?: true,
      timeout: spec.request_timeout,
      long_poll: nil,
      wait?: false,
      deadline: now() + spec.request_timeout
    }
  end

  defp carrying(dispatch, request), do: %{dispatch | request: request, info: @protocol.request_info(request)}

  defp not_written(spec, address, :not_connected) do
    Logger.debug("#{describe(spec, address)} not connected, skipping")
  end

  defp not_written(spec, address, reason), do: log_failed(spec, address, reason)

  defp log_failed(spec, address, reason) do
    Logger.warning("Network call to #{describe(spec, address)} failed: #{inspect(refusal(reason))}")
  end

  defp refusal({:unsupported_version, _versions}), do: :api_version_no_supported
  defp refusal(reason), do: reason

  defp call(fun) do
    fun.()
  catch
    :exit, {:timeout, _call} -> {:error, :timeout}
    :exit, _reason -> {:error, :not_connected}
  end

  defp call_connection(fun, dropped) do
    fun.()
  catch
    :exit, {:timeout, _call} -> {:error, :timeout, @no_bytes}
    :exit, _reason -> {:error, dropped, @no_bytes}
  end

  defp build_metadata(versions, spec), do: build(:metadata, &RequestBuilder.metadata_request/2, versions, spec)

  defp build_find_coordinator(versions, spec) do
    build(:find_coordinator, &RequestBuilder.find_coordinator_request/2, versions, spec)
  end

  # The builder logs a missing API on every call, and a round builds per candidate and per attempt.
  defp build(api, builder, versions, spec) do
    state = state(versions)

    if Process.get(@unsupported_logged, false) and
         State.max_supported_api_version(state, api) == {:error, :api_not_supported_by_broker} do
      {:error, :api_not_supported_by_broker}
    else
      result = builder.(spec.request_opts, state)
      if result == {:error, :api_not_supported_by_broker}, do: Process.put(@unsupported_logged, true)
      result
    end
  end

  defp log_build_failure(%{kind: :find_coordinator} = spec, reason) do
    Logger.warning(
      "Unable to build find_coordinator request for #{inspect(spec.request_opts[:group_id])}: " <>
        "Error #{inspect(reason)}"
    )
  end

  defp log_build_failure(_spec, _reason), do: Logger.error("Unable to build metadata request.")

  defp state(versions), do: struct(State, api_versions: versions)

  defp parse_coordinator(response, spec) do
    case ResponseParser.find_coordinator_response(response) do
      {:ok, %FindCoordinator{} = coordinator} ->
        coordinator

      {:error, error} ->
        log_coordinator_failure(spec, error)
        {:error, error}
    end
  end

  defp log_coordinator_failure(spec, reason) do
    Logger.warning(
      "Unable to find consumer group coordinator for #{inspect(spec.request_opts[:group_id])}: " <>
        "Error #{inspect(reason)}"
    )
  end

  defp broker_info(spec, {host, port} = address) do
    case Map.get(spec.brokers, address) do
      %Broker{} = broker -> %{node_id: broker.node_id, host: broker.host, port: broker.port, rack: broker.rack}
      nil -> %{node_id: nil, host: host, port: port, rack: nil}
    end
  end

  defp describe(spec, {host, port} = address) do
    "broker #{broker_info(spec, address).node_id} (#{host}:#{port})"
  end

  defp now, do: System.monotonic_time(:millisecond)
end
