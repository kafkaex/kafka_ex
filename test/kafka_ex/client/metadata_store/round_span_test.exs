defmodule KafkaEx.Client.MetadataStore.RoundSpanTest.ThrowingName do
  @moduledoc false
  def whereis_name(name), do: throw({:whereis_threw, name})
end

defmodule KafkaEx.Client.MetadataStore.RoundSpanTest do
  use KafkaEx.TestSupport.StoreCase, async: false

  alias KafkaEx.Client.MetadataStore.Round

  @metadata_v0 %{3 => {0, 0}}
  @no_bytes %{bytes_sent: 0, bytes_received: 0}

  setup do
    ctx = start_fake([{1, 9092}, {2, 9093}], topics: %{"pin" => [1, 2]}, api_versions: versions())

    %{store: store, gate: gate} =
      start_store(ctx, bootstrap: [9092], store: [backoff_base: 60_000, backoff_cap: 60_000])

    await!(store)
    watch_request_events()
    Map.merge(ctx, %{store: store, gate: gate})
  end

  test "a walk whose only candidate never connects emits no request start", ctx do
    PinGate.close(ctx.gate)
    candidates = [candidate(ctx, 9093)]

    {result, _log} = with_log(fn -> Round.run(round_spec(candidates, connect_timeout: 200)) end)

    assert result == {:error, :no_broker}
    refute_received {:request, _phase, _metadata}
  end

  test "a walk whose candidate is up at connect but down when the request goes out emits no request events" do
    down = {:error, :not_connected, @no_bytes}
    candidate = stand_in("down-after-connect", %{connect: {:ok, @metadata_v0}, await: down, request: down})

    {result, _log} = with_log(fn -> Round.run(round_spec([candidate], [])) end)

    assert result == {:error, :broker_failed}
    refute_received {:request, _phase, _metadata}
  end

  test "a walk whose connected candidate refuses the version before the write emits no request events", ctx do
    spec = round_spec([candidate(ctx, 9092)], kind: :find_coordinator, request_opts: [group_id: "g", api_version: 0])

    {result, log} = with_log(fn -> Round.run(spec) end)

    assert result == {:error, :api_version_no_supported}
    refute_received {:request, _phase, _metadata}
    assert count(ctx.fake, :find_coordinator) == 0
    assert log =~ "(#{ctx.host}:9092) failed: :api_version_no_supported\n"
    refute log =~ "unsupported_version"
  end

  test "a request a connection refuses after a reconnect is rebuilt at that connection's versions and written" do
    refused = {:error, {:unsupported_version, %{3 => {0, 0}}}, @no_bytes}

    candidate =
      stand_in("reconnected", %{
        connect: {:ok, %{3 => {0, 1}}},
        await: [refused, :ok],
        write: {:error, :closed, %{bytes_sent: 10, bytes_received: 0, correlation_id: 3}}
      })

    {result, _log} = with_log(fn -> Round.run(round_spec([candidate], [])) end)

    assert result == {:error, :broker_failed}
    assert_received {:request, :start, %{operation: :metadata, api_version: 0}}
    assert_received {:request, :stop, %{operation: :metadata, api_version: 0, bytes_sent: 10}}
    refute_received {:request, _phase, _metadata}
  end

  test "a refusal at the write, after a reconnect, is logged by its atom and the walk moves on" do
    refused = {:error, {:unsupported_version, %{3 => {1, 1}}}, @no_bytes}
    candidate = stand_in("refuses-at-write", %{connect: {:ok, @metadata_v0}, await: :ok, write: refused})

    {result, log} = with_log(fn -> Round.run(round_spec([candidate], [])) end)

    assert result == {:error, :broker_failed}
    assert log =~ "(refuses-at-write:9092) failed: :api_version_no_supported\n"
    refute log =~ "unsupported_version"
  end

  test "a refusal ends the walk only when no candidate wrote" do
    candidates = [
      stand_in("closed-on-write", %{connect: {:ok, @metadata_v0}, await: :ok, write: {:error, :closed, @no_bytes}}),
      stand_in("refuses", %{connect: {:ok, @metadata_v0}, await: {:error, {:unsupported_version, %{}}, @no_bytes}})
    ]

    {result, _log} = with_log(fn -> Round.run(round_spec(candidates, [])) end)

    assert result == {:error, :broker_failed}
  end

  test "a boot whose every candidate refuses the version still boots with the bootstrap brokers" do
    refuses = %{connect: {:ok, %{3 => {1, 1}}}, await: {:error, {:unsupported_version, %{3 => {1, 1}}}, @no_bytes}}
    request_opts = [topics: [], allow_auto_topic_creation: false, api_version: 0]
    spec = round_spec([stand_in("refuses", refuses)], kind: :load, request_opts: request_opts)

    {result, _log} = with_log(fn -> Round.run(spec) end)

    assert {:ok, %{metadata: %{brokers: %{1 => %{host: "refuses", port: 9092}}}}} = result
    refute_received {:request, _phase, _metadata}
  end

  test "a request the connection cannot encode ends the walk with one exception event and no stacktrace logged",
       ctx do
    candidates = [candidate(ctx, 9092), candidate(ctx, 9093)]
    spec = round_spec(candidates, kind: :find_coordinator, request_opts: [group_id: 123])

    {result, log} = with_log(fn -> Round.run(spec) end)

    assert result == {:error, :broker_failed}
    assert_received {:request, :start, %{operation: :find_coordinator}}

    assert_received {:request, :exception,
                     %{operation: :find_coordinator, kind: :error, reason: _reason, stacktrace: [_ | _]}}

    refute_received {:request, _phase, _metadata}
    refute Enum.any?(FakeTransport.connects(ctx.fake), &(&1.port == 9093))
    assert count(ctx.fake, :find_coordinator) == 0
    assert log =~ "(#{ctx.host}:9092) failed: {:exception, :error, %Kayrock.InvalidRequestError{"
    refute log =~ "line:"
    refute log =~ ~r/\.ex:\d+:/
  end

  test "a drop between await and write counts as written: one span, stopped with the walk's failure" do
    candidates = [
      stand_in("exits-on-write", %{connect: {:ok, @metadata_v0}, await: :ok, write: :exit}),
      stand_in("closed-on-write", %{
        connect: {:ok, @metadata_v0},
        await: :ok,
        write: {:error, :closed, %{bytes_sent: 10, bytes_received: 3, correlation_id: 7}}
      })
    ]

    {result, log} = with_log(fn -> Round.run(round_spec(candidates, [])) end)

    assert result == {:error, :broker_failed}
    assert_received {:request, :start, %{operation: :metadata}}

    assert_received {:request, :stop,
                     %{
                       result: :error,
                       error: :broker_failed,
                       broker: %{},
                       bytes_sent: 10,
                       bytes_received: 0,
                       correlation_id: 7
                     }}

    refute_received {:request, _phase, _metadata}
    assert log =~ "(exits-on-write:9092) failed: :closed"
    assert log =~ "(closed-on-write:9092) failed: :closed"
  end

  test "a raise after the span opens ends it with the request exception" do
    candidates = [
      stand_in("closed-on-write", %{connect: {:ok, @metadata_v0}, await: :ok, write: {:error, :closed, @no_bytes}}),
      stand_in("raises-on-connect", %{connect: :unexpected})
    ]

    {result, _log} = with_log(fn -> Round.run(round_spec(candidates, [])) end)

    assert {:raised, {:error, %CaseClauseError{}, [_ | _]}} = result
    assert_received {:request, :start, %{operation: :metadata}}

    assert_received {:request, :exception,
                     %{operation: :metadata, kind: :error, reason: {:case_clause, :unexpected}, stacktrace: [_ | _]}}

    refute_received {:request, _phase, _metadata}
  end

  # A via-name lookup runs in the walk's process, so its throw reaches the walk past the connection call.
  test "a throw after the span opens ends it with the request exception and the raw reason" do
    candidates = [
      stand_in("closed-on-write", %{connect: {:ok, @metadata_v0}, await: :ok, write: {:error, :closed, @no_bytes}}),
      {{"throws-on-connect", 9092}, {:via, __MODULE__.ThrowingName, :conn}}
    ]

    {thrown, _log} = with_log(fn -> catch_throw(Round.run(round_spec(candidates, []))) end)

    assert thrown == {:whereis_threw, :conn}
    assert_received {:request, :start, %{operation: :metadata}}

    assert_received {:request, :exception, %{operation: :metadata, kind: :throw, reason: ^thrown, stacktrace: [_ | _]}}

    refute_received {:request, _phase, _metadata}
  end

  defp candidate(ctx, port) do
    {:ok, conn} = MetadataStore.connection(ctx.store, {ctx.host, port}, 1_000)
    {{ctx.host, port}, conn}
  end

  defp round_spec(candidates, overrides) do
    Map.merge(
      %{
        kind: :metadata,
        candidates: candidates,
        brokers: %{},
        request_opts: [topics: [], allow_auto_topic_creation: false],
        known_missing: MapSet.new(),
        ladder_delay: 10,
        connect_timeout: 1_000,
        connect_slack: 1_500,
        request_timeout: 2_000,
        request_slack: 1_000,
        deadline: System.monotonic_time(:millisecond) + 60_000
      },
      Map.new(overrides)
    )
  end

  # A scripted Connection: each call kind gets its reply (a list is answered in order), or :exit stops it mid-call.
  defp stand_in(host, replies), do: {{host, 9092}, spawn_link(fn -> answer(replies) end)}

  defp answer(replies) do
    receive do
      {:"$gen_call", from, call} ->
        kind = call_kind(call)

        case Map.fetch!(replies, kind) do
          :exit -> exit(:normal)
          [reply | rest] -> answer(reply, from, Map.put(replies, kind, if(rest == [], do: [reply], else: rest)))
          reply -> answer(reply, from, replies)
        end
    end
  end

  defp answer(reply, from, replies) do
    :gen_statem.reply(from, reply)
    answer(replies)
  end

  defp call_kind({kind, _dispatch}), do: kind
  defp call_kind(kind), do: kind

  defp watch_request_events do
    test = self()
    handler = "round-span-#{System.unique_integer([:positive])}"

    :telemetry.attach_many(
      handler,
      [[:kafka_ex, :request, :start], [:kafka_ex, :request, :stop], [:kafka_ex, :request, :exception]],
      fn [:kafka_ex, :request, phase], _measurements, metadata, _config -> send(test, {:request, phase, metadata}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)
  end
end
