defmodule KafkaEx.Client.RequestSpanTest do
  use KafkaEx.TestSupport.PinCase, async: false

  alias KafkaEx.Client.NodeSelector
  alias KafkaEx.Client.RequestBuilder
  alias KafkaEx.Client.State
  alias KafkaEx.Client.Supervisor, as: ClientSupervisor
  alias KafkaEx.Messages.RecordMetadata

  setup do
    pinned = start_pinned(connection: [reconnect_window: 200])
    events = for phase <- [:start, :stop, :exception], do: [:kafka_ex, :request, phase]
    forward_telemetry(events, fn [_, _, phase], _measurements, metadata -> {:request_span, phase, metadata} end)
    pinned
  end

  test "the request :start event is emitted before the reply arrives", %{fake: fake, client: client} do
    FakeTransport.respond(fake, :produce, produce_reply())
    FakeTransport.hold(fake, :produce)
    :ok = FakeTransport.subscribe(fake)

    produce = Task.async(fn -> API.produce(client, @topic, 0, [%{value: "v"}]) end)

    assert_receive {:request_span, :start, %{operation: :produce}}, 5_000
    assert_receive {:fake_transport, :request, %{api: :produce}}, 5_000
    refute_received {:request_span, :stop, _}

    FakeTransport.release(fake, :produce)

    assert {:ok, _} = Task.await(produce, 5_000)
    assert_receive {:request_span, :stop, %{operation: :produce, result: :ok}}, 1_000
  end

  test "a :first_available walk whose first candidate fails is one span and one correlation id", %{
    fake: fake,
    client: client,
    host: host
  } do
    FakeTransport.respond(fake, :find_coordinator, fn request ->
      if request.port == 9092, do: :close, else: %{node_id: 2, host: host, port: 9093, error_message: nil}
    end)

    {:ok, before} = API.correlation_id(client)

    {result, log} = with_log(fn -> API.find_coordinator(client, "g") end)

    assert {:ok, %{coordinator: %{node_id: 2}}} = result
    assert log =~ "Network call to broker 1 (#{host}:9092) failed: :closed\n"
    assert nodes(fake, :find_coordinator) |> Enum.take(-2) == [1, 2]
    assert_receive {:request_span, :start, %{operation: :find_coordinator, correlation_id: ^before}}, 1_000
    assert_receive {:request_span, :stop, %{result: :ok, broker: %{port: 9093}}}, 1_000
    refute_receive {:request_span, _, _}, 100
    assert API.correlation_id(client) == {:ok, before + 1}
  end

  test "a store walk whose first candidate fails is one span from the store and spends no client correlation id", %{
    fake: fake,
    client: client,
    host: host
  } do
    FakeTransport.respond(fake, :metadata, fn request ->
      if request.port == 9092, do: :close, else: metadata_reply(host, [{@topic, [1, 2]}])
    end)

    {:ok, before} = API.correlation_id(client)

    {result, log} = with_log(fn -> API.topics_metadata(client, [@topic]) end)

    assert {:ok, [%{name: @topic}]} = result
    assert log =~ "Network call to broker 1 (#{host}:9092) failed: :closed\n"
    assert nodes(fake, :metadata) |> Enum.take(-2) == [1, 2]
    assert_receive {:request_span, :start, %{operation: :metadata, correlation_id: nil}}, 1_000
    assert_receive {:request_span, :stop, %{result: :ok, broker: %{port: 9093}}}, 1_000
    refute_receive {:request_span, _, _}, 100
    assert API.correlation_id(client) == {:ok, before}
  end

  test "an attempt that never reaches a connection emits no span and spends no correlation id", %{
    fake: fake,
    client: client,
    host: host,
    gate: gate
  } do
    watch_close(host)
    drop_connection(fake, host)
    PinGate.close(gate)
    {:ok, before} = API.correlation_id(client)

    {result, _log} = with_log(fn -> API.produce(client, @topic, 0, [%{value: "v"}]) end)

    assert result == {:error, :no_broker}
    refute_receive {:request_span, _, _}, 100
    assert API.correlation_id(client) == {:ok, before}
  end

  test "a request the connection cannot encode emits the request exception and spares the connection", %{
    fake: fake,
    client: client,
    host: host
  } do
    watch_close(host)
    FakeTransport.respond(fake, :produce, produce_reply())

    {result, _log} = with_log(fn -> API.produce(client, @topic, 0, [%{value: 123}]) end)

    assert result == {:error, :timeout}
    assert_receive {:request_span, :start, %{operation: :produce}}, 1_000

    assert_receive {:request_span, :exception,
                    %{operation: :produce, kind: :error, reason: _reason, stacktrace: [_ | _]}},
                   1_000

    refute_received {:request_span, :stop, _}
    assert requests(fake, :produce) == []

    assert {:ok, %RecordMetadata{base_offset: 7}} = API.produce(client, @topic, 0, [%{value: "v"}])
    refute_received {:pin_closed, _}
    assert [%{conn: conn}] = requests(fake, :produce)
    assert [%{owner: connection}] = Enum.filter(FakeTransport.connects(fake), &(&1.role == :data))
    assert %{owner: ^connection} = Enum.find(FakeTransport.connections(fake), &(&1.id == conn))
  end

  test "a retryable request the connection cannot encode fails at once, with one exception event", %{
    fake: fake,
    client: client
  } do
    fetches_before = count(fake, :fetch)

    {result, _log} = with_log(fn -> API.fetch(client, @topic, 0, 0, max_wait_time: 10, max_bytes: :unencodable) end)

    assert result == {:error, :timeout}
    assert_receive {:request_span, :start, %{operation: :fetch}}, 1_000
    assert_receive {:request_span, :exception, %{operation: :fetch, kind: :error}}, 1_000
    refute_receive {:request_span, _, _}, 100
    assert count(fake, :fetch) == fetches_before
  end

  describe "a crash after the span opens" do
    test "a raise ends the span with the request exception", %{fake: fake, client: client} do
      ref = Process.monitor(client)
      warm_data_connection(fake, client)
      FakeTransport.respond(fake, :produce, :close)
      fail_connection_start()

      {exit, _log} =
        with_log(fn -> catch_exit(Client.send_request(client, produce_request(), NodeSelector.first_available())) end)

      assert {{{:case_clause, {:error, _}}, [_ | _] = stacktrace} = reason, _call} = exit
      assert_receive {:DOWN, ^ref, :process, ^client, ^reason}, 5_000
      assert_received {:request_span, :start, %{operation: :produce}}

      assert_received {:request_span, :exception,
                       %{
                         operation: :produce,
                         kind: :error,
                         reason: {:case_clause, {:error, _}},
                         stacktrace: ^stacktrace
                       }}

      refute_received {:request_span, _, _}
    end

    test "an exit ends the span with the request exception", %{fake: fake, client: client} do
      ref = Process.monitor(client)
      warm_data_connection(fake, client)
      FakeTransport.respond(fake, :produce, :close)
      supervisor = ClientSupervisor.connection_supervisor(:sys.get_state(client).supervisor)
      :sys.replace_state(supervisor, fn _state -> :broken end)

      {exit, _log} =
        with_log(fn -> catch_exit(Client.send_request(client, produce_request(), NodeSelector.first_available())) end)

      assert {{_crash, {GenServer, :call, [^supervisor, {:start_child, _child}, :infinity]}} = reason, _call} = exit
      assert_receive {:DOWN, ^ref, :process, ^client, ^reason}, 5_000
      assert_received {:request_span, :start, %{operation: :produce}}

      assert_received {:request_span, :exception,
                       %{operation: :produce, kind: :exit, reason: ^reason, stacktrace: stacktrace}}

      assert Enum.any?(stacktrace, &client_frame?(&1, :start_connection, 2))

      refute_received {:request_span, _, _}
    end
  end

  # A walk tries connected brokers first, so the fault meets the second candidate's connection start.
  defp warm_data_connection(fake, client) do
    FakeTransport.respond(fake, :produce, produce_reply())
    {:ok, _} = API.produce(client, @topic, 0, [%{value: "v"}])
    assert_receive {:request_span, :stop, %{operation: :produce}}, 1_000
    assert_received {:request_span, :start, %{operation: :produce}}
  end

  defp produce_request do
    opts = [topic: @topic, partition: 0, messages: [%{value: "v"}], acks: 1, timeout: 1_000]

    {:ok, request} = RequestBuilder.produce_request(opts, %State{api_versions: %{0 => {3, 3}}})
    request
  end

  # Once a test stubs KafkaEx.Client, Mimic runs the client's code under a renamed module.
  defp client_frame?({_module, function, arity, location}, function, arity) do
    location[:file] == ~c"lib/kafka_ex/client/client.ex"
  end

  defp client_frame?(_frame, _function, _arity), do: false
end
