defmodule KafkaEx.Client.MetadataStore.Round do
  @moduledoc """
  One metadata or FindCoordinator request, walked across the candidate brokers in order. The store runs `run/1`
  in a separate monitored process that sends the result back to the store. A candidate that fails to connect,
  cannot take the request at its versions, times out or answers with an unparsable response passes the walk to
  the next one. The walk opens one request span, at its first write, and closes it with the result. A metadata
  answer that misses a newly requested topic is retried after a delay while the round's bound allows.
  """

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
          {:ok, %{required(:address) => address(), optional(atom()) => term()}}
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
    builder = fn versions -> build(:find_coordinator, &RequestBuilder.find_coordinator_request/2, versions, spec) end

    case first_response(spec, builder) do
      {:ok, response, address, _versions} -> {:ok, %{coordinator: parse_coordinator(response, spec), address: address}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp metadata(spec, attempts_left) do
    started = now()

    builder = fn versions -> build(:metadata, &RequestBuilder.metadata_request/2, versions, spec) end

    case first_response(spec, builder) do
      {:ok, response, address, versions} -> parsed(spec, response, address, versions, attempts_left, now() - started)
      {:bootstrap, address, versions} -> bootstrap_only(spec, address, versions)
      {:error, reason} -> {:error, reason}
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
          unknown =
            for {topic, :unknown_topic_or_partition} <- ResponseParser.metadata_topic_errors(response), do: topic

          {:ok, %{metadata: metadata, unknown: unknown, missing: missing, address: address, versions: versions}}
        end

      {:error, _reason} ->
        Logger.error("Unable to parse metadata response from brokers.")
        {:error, :parse_error}
    end
  end

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
    start = %{failures: [], span: nil}

    case Enum.reduce_while(spec.candidates, start, &visit(&1, spec, build, &2)) do
      {:answered, response, address, versions} ->
        {:ok, response, address, versions}

      walk ->
        if walk.span do
          stop_span(%{walk.span | meta: %{walk.span.meta | bytes_received: 0}}, {:error, :broker_failed}, %{})
        end

        give_up(spec, Enum.reverse(walk.failures))
    end
  end

  defp visit({address, pid}, spec, build, walk) do
    case under_span(walk, fn -> prepare(spec, build, address, pid) end) do
      {:skip, failure} ->
        {:cont, record(spec, walk, failure)}

      {:ready, dispatch, versions} ->
        walk = begin_request(walk, dispatch.request, {address, versions})
        under_span(walk, fn -> exchange(spec, walk, {address, pid, versions}, dispatch) end)
    end
  end

  defp prepare(spec, build, address, pid) do
    case connect(spec, address, pid, @api_versions_max_retries) do
      {:connect_error, reason} ->
        {:skip, {:not_connected, reason}}

      {:ok, versions} ->
        case build.(versions) do
          {:error, reason} ->
            {:skip, {:not_built, {address, versions}, reason}}

          {:ok, request} ->
            ready(spec, build, {address, pid, versions}, new_dispatch(spec, request(request), address))
        end
    end
  end

  defp ready(spec, build, {address, pid, versions}, dispatch) do
    case negotiated(spec, build, pid, versions, dispatch) do
      {:ok, dispatch, versions} ->
        {:ready, dispatch, versions}

      {:refused, reason, refused_versions} ->
        log_failed(spec, address, reason)
        {:skip, {:refused, {address, refused_versions}, reason}}

      {:error, reason, _meta} ->
        not_written(spec, address, reason)
        {:skip, {:connected, {address, versions}}}
    end
  end

  defp negotiated(spec, build, pid, versions, dispatch) do
    case await_connection(spec, pid, dispatch) do
      :ok -> {:ok, dispatch, versions}
      {:error, {:unsupported_version, versions}, _meta} -> rebuild(spec, build, pid, versions, dispatch)
      error -> error
    end
  end

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

  defp exchange(spec, walk, {address, pid, versions}, dispatch) do
    left = max(dispatch.deadline + spec.request_slack - now(), 0)

    case call_connection(fn -> ConnectionStateMachine.write(pid, dispatch, left) end, :closed) do
      {:ok, frame, meta} ->
        case ResponseParser.deserialize(frame, dispatch.request) do
          {:ok, response} ->
            stop_span(%{walk.span | meta: meta}, {:ok, response}, broker_info(spec, address))
            {:halt, {:answered, response, address, versions}}

          {:error, :parse_error} ->
            {:cont, put_meta(walk, meta)}
        end

      # Encoding is deterministic: no other candidate can do better.
      {:error, {:exception, kind, reason, stacktrace}, _meta} ->
        Logger.warning("Network call to #{describe(spec, address)} failed: #{inspect({:exception, kind, reason})}")
        Telemetry.exception_span(walk.span.handle, walk.span.metadata, kind, reason, stacktrace)
        {:halt, %{walk | span: nil}}

      {:error, reason, meta} ->
        log_failed(spec, address, reason)
        {:cont, put_meta(walk, meta)}
    end
  end

  defp begin_request(%{span: nil} = walk, request, written_at) do
    metadata = Telemetry.request_metadata(request, %{})
    handle = Telemetry.start_span([:kafka_ex, :request], metadata)
    span = %{handle: handle, start_metadata: metadata, metadata: metadata, meta: @no_bytes}
    %{walk | span: span, failures: [{:written, written_at} | walk.failures]}
  end

  defp begin_request(%{span: span} = walk, request, _written_at) do
    %{walk | span: %{span | metadata: Telemetry.request_metadata(request, %{})}}
  end

  defp put_meta(walk, meta), do: %{walk | span: %{walk.span | meta: meta}}

  defp stop_span(%{handle: handle, metadata: metadata, meta: meta}, result, broker) do
    Telemetry.stop_span(handle, {result, Map.merge(metadata, Map.put(meta, :broker, broker))})
  end

  # A crash while a span is open still ends it.
  defp under_span(%{span: nil}, fun), do: fun.()

  defp under_span(%{span: span}, fun) do
    fun.()
  catch
    kind, reason ->
      Telemetry.exception_span(span.handle, span.start_metadata, kind, reason, __STACKTRACE__)
      :erlang.raise(kind, reason, __STACKTRACE__)
  end

  # One build-failure line per walk, as the client logged one per refresh.
  defp record(spec, walk, {:not_built, _at, reason} = failure) do
    if not Enum.any?(walk.failures, &match?({:not_built, _at, _reason}, &1)), do: log_build_failure(spec, reason)
    %{walk | failures: [failure | walk.failures]}
  end

  defp record(_spec, walk, failure), do: %{walk | failures: [failure | walk.failures]}

  defp give_up(spec, failures) do
    case culprit(failures) do
      {:not_built, at, reason} ->
        settle(spec, at, reason)

      {:not_connected, reason} ->
        reported = if spec.kind == :load, do: reason, else: :no_broker
        log_all_failed(spec, reported)
        {:error, reported}

      culprit ->
        {at, reason} = failed_at(culprit)
        log_all_failed(spec, reason)
        settle(spec, at, reason)
    end
  end

  defp failed_at({:refused, at, reason}), do: {at, reason}
  defp failed_at({_kind, at}), do: {at, :broker_failed}

  # Precedence: a write, the last refusal, a connect without a write, a build failure, then the connect errors.
  defp culprit(failures) do
    first_of(failures, :written) || first_of(Enum.reverse(failures), :refused) || first_of(failures, :connected) ||
      first_of(failures, :not_built) || connect_failure(failures)
  end

  defp first_of(failures, kind), do: Enum.find(failures, &(elem(&1, 0) == kind))

  defp connect_failure(failures) do
    reasons = for {:not_connected, reason} <- failures, do: reason
    {:not_connected, Enum.find(reasons, &match?({:api_versions, _}, &1)) || List.last(reasons) || :no_broker}
  end

  defp settle(%{kind: :load}, {address, versions}, _reason), do: {:bootstrap, address, versions}
  defp settle(_spec, _at, reason), do: {:error, reason}

  defp log_all_failed(%{kind: :find_coordinator} = spec, reason), do: log_coordinator_failure(spec, reason)

  defp log_all_failed(spec, _reason) do
    Logger.error("Unable to fetch metadata from any brokers. Timeout is #{spec.request_timeout}.")
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

  defp new_dispatch(spec, request, address) do
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
    case call_connection(fun, :not_connected) do
      {:error, reason, _meta} -> {:error, reason}
      reply -> reply
    end
  end

  defp call_connection(fun, dropped) do
    fun.()
  catch
    :exit, {:timeout, _call} -> {:error, :timeout, @no_bytes}
    :exit, _reason -> {:error, dropped, @no_bytes}
  end

  # The builder logs a missing API on every call, and a round builds per candidate and per attempt.
  defp build(api, builder, versions, spec) do
    state = struct(State, api_versions: versions)

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
