defmodule KafkaEx.Pins.RequestSpanPinTest do
  use KafkaEx.TestSupport.PinCase, async: false

  @moduletag pin_set: :invariant

  @stop_keys [
    :api_version,
    :broker,
    :bytes_received,
    :bytes_sent,
    :client_id,
    :correlation_id,
    :operation,
    :result,
    :telemetry_span_context
  ]

  defp capture_requests(host) do
    test = self()
    handler = "pin-span-#{host}-#{System.unique_integer([:positive])}"

    :telemetry.attach_many(
      handler,
      [[:kafka_ex, :request, :start], [:kafka_ex, :request, :stop]],
      fn [:kafka_ex, :request, phase], _measurements, metadata, _ ->
        send(test, {:span, phase, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)
  end

  # Ends on 200 ms of silence, and only after the caller's results are in.
  defp drain_spans(acc \\ []) do
    receive do
      {:span, phase, metadata} -> drain_spans([{phase, metadata} | acc])
    after
      200 -> Enum.reverse(acc)
    end
  end

  defp spans_of(spans, operation), do: Enum.filter(spans, fn {_phase, metadata} -> metadata.operation == operation end)

  defp stop_keys(spans, operation) do
    assert [{:start, _start}, {:stop, stop}] = spans_of(spans, operation)
    stop |> Map.keys() |> Enum.sort()
  end

  test "one request span per written request, per API and per broker" do
    %{fake: fake, client: client, host: host} = start_pinned()
    FakeTransport.respond(fake, :produce, produce_reply())
    FakeTransport.respond(fake, :fetch, &{:frame, fetch_frame(&1, -1)})
    capture_requests(host)

    assert {:ok, _} = API.produce(client, @topic, 0, [%{value: "a"}])
    assert {:ok, _} = API.produce(client, @topic, 0, [%{value: "b"}])
    assert {:ok, _} = API.produce(client, @topic, 1, [%{value: "c"}])
    assert {:ok, _} = API.fetch(client, @topic, 0, 0, max_wait_time: 10)

    spans = drain_spans()
    apis = [:produce, :fetch]
    ours = Enum.filter(spans, fn {_phase, metadata} -> metadata.operation in apis end)

    stops =
      for({:stop, metadata} <- ours, do: {metadata.operation, metadata.broker.node_id}) |> Enum.frequencies()

    written =
      for(%{api: api, node_id: node_id} <- FakeTransport.requests(fake), api in apis, do: {api, node_id})
      |> Enum.frequencies()

    assert stops == written
    assert Enum.count(ours, &match?({:start, _}, &1)) == Enum.count(ours, &match?({:stop, _}, &1))
  end

  describe "request stop metadata keys per outcome" do
    test "a success" do
      %{fake: fake, client: client, host: host} = start_pinned()
      FakeTransport.respond(fake, :produce, produce_reply())
      capture_requests(host)

      assert {:ok, _} = API.produce(client, @topic, 0, [%{value: "v"}])

      spans = drain_spans()
      assert stop_keys(spans, :produce) == @stop_keys
      assert [{:start, %{broker: %{}}}, {:stop, %{broker: broker}}] = spans_of(spans, :produce)
      assert broker |> Map.keys() |> Enum.sort() == [:host, :node_id, :port, :rack]
    end

    test "acks 0" do
      %{fake: fake, client: client, host: host} = start_pinned()
      :ok = FakeTransport.subscribe(fake)
      capture_requests(host)

      assert {:ok, _} = API.produce(client, @topic, 0, [%{value: "v"}], acks: 0)
      assert_receive {:fake_transport, :request, %{api: :produce}}, 5_000

      spans = drain_spans()
      assert stop_keys(spans, :produce) == @stop_keys
      assert [{:start, %{broker: broker}}, _stop] = spans_of(spans, :produce)
      assert broker == %{}
    end

    test "a reply timeout" do
      with_request_timeout(200)
      %{fake: fake, client: client, host: host} = start_pinned()
      FakeTransport.respond(fake, :produce, :no_reply)
      capture_requests(host)

      {result, _log} = with_log(fn -> API.produce(client, @topic, 0, [%{value: "v"}]) end)

      assert result == {:error, :timeout}
      assert stop_keys(drain_spans(), :produce) == Enum.sort([:error | @stop_keys])
    end

    test "a peer close" do
      %{fake: fake, client: client, host: host} = start_pinned()
      FakeTransport.respond(fake, :produce, :close)
      capture_requests(host)

      {result, _log} = with_log(fn -> API.produce(client, @topic, 0, [%{value: "v"}]) end)

      assert result == {:error, :closed}
      assert stop_keys(drain_spans(), :produce) == Enum.sort([:error | @stop_keys])
    end

    test "an unparsable reply" do
      %{fake: fake, client: client, host: host} = start_pinned()
      FakeTransport.respond(fake, :produce, &{:frame, <<&1.correlation_id::32, 0xFF>>})
      capture_requests(host)

      {result, _log} = with_log(fn -> API.produce(client, @topic, 0, [%{value: "v"}]) end)

      assert result == {:error, :parse_error}
      assert stop_keys(drain_spans(), :produce) == Enum.sort([:error | @stop_keys])
    end
  end

  describe "a request that is never written emits no request span" do
    test "no broker for the topic" do
      %{client: client, host: host} = start_pinned()
      capture_requests(host)

      {result, _log} = with_log(fn -> API.produce(client, "no-such-topic", 0, [%{value: "v"}]) end)

      assert result == {:error, :no_broker}
      assert spans_of(drain_spans(), :produce) == []
    end

    test "a version pin the client cannot build" do
      %{fake: fake, client: client, host: host} = start_pinned()
      capture_requests(host)

      {result, _log} = with_log(fn -> API.produce(client, @topic, 0, [%{value: "v"}], api_version: 99) end)

      assert result == {:error, :api_version_no_supported}
      assert spans_of(drain_spans(), :produce) == []
      assert count(fake, :produce) == 0
    end
  end

  test "the [:kafka_ex, :request] start metadata carries the client-wide correlation id" do
    %{fake: fake, client: client, host: host} = start_pinned()
    FakeTransport.respond(fake, :produce, produce_reply())
    # Moves the client-wide id past any id a fresh node-1 connection would start from.
    for _ <- 1..5, do: {:ok, _} = API.produce(client, @topic, 1, [%{value: "v"}])
    test = self()
    handler = "pin-request-#{host}"

    :telemetry.attach(
      handler,
      [:kafka_ex, :request, :start],
      fn _event, _measurements, metadata, _config -> send(test, {:pin_request, metadata.correlation_id}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)

    {:ok, expected} = API.correlation_id(client)
    {:ok, _} = API.produce(client, @topic, 0, [%{value: "v"}])

    assert_received {:pin_request, ^expected}
  end

  test "an unparsable reply is written once, logged once and returned as a parse error" do
    %{fake: fake, client: client} = start_pinned()
    FakeTransport.respond(fake, :produce, &{:frame, <<&1.correlation_id::32, 0xFF>>})

    {result, log} = with_log(fn -> API.produce(client, @topic, 0, [%{value: "v"}]) end)

    assert result == {:error, :parse_error}
    assert count(fake, :produce, 1) == 1
    assert length(Regex.scan(~r/Failed to parse a response from the server/, log)) == 1
  end
end
