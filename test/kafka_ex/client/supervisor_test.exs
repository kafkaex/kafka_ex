defmodule KafkaEx.Client.SupervisorTest do
  use KafkaEx.TestSupport.PinCase, async: false

  alias KafkaEx.Client.ConnectionStateMachine
  alias KafkaEx.Client.MetadataStore
  alias KafkaEx.Client.Supervisor, as: ClientSupervisor
  alias KafkaEx.Messages.RecordMetadata

  test "the client supervisor restarts rest-for-one, at most 3 times in 5 seconds, with the connection registry first" do
    assert {:ok, {flags, children}} = ClientSupervisor.init(%{store: %{}})
    assert %{strategy: :rest_for_one, intensity: 3, period: 5} = flags

    assert [%{id: :connection_registry}, %{id: :connections, type: :supervisor}, %{id: :metadata_store}] =
             children
  end

  test "the metadata store is a transient child with a 5 second shutdown" do
    assert {:ok, {_flags, [_registry, _connections, store]}} = ClientSupervisor.init(%{store: %{}})
    assert %{id: :metadata_store, restart: :transient, shutdown: 5_000} = store
  end

  test "a client boots without closing the bootstrap sockets and closes them with :shutdown on stop" do
    %{fake: fake, host: host} = start_fake([{1, 9092}])
    watch_close(host)

    {:ok, client} =
      Client.start_link(
        [uris: [{host, 9092}], consumer_group: :no_consumer_group, ssl_options: FakeTransport.ssl_options(fake)],
        :no_name
      )

    assert {:ok, %{brokers: %{1 => %{socket: nil}}}} = API.cluster_metadata(client)
    refute_receive {:pin_closed, _}, 100

    GenServer.stop(client)
    assert_receive {:pin_closed, %{reason: :shutdown}}, 1_000
  end

  test "a boot that fails closes every connection it opened, once, and leaves no process behind" do
    Process.flag(:trap_exit, true)
    %{fake: fake, host: host} = start_fake([{1, 9092}, {2, 9093}], api_versions: %{metadata: 12..12})
    FakeTransport.respond(fake, :metadata, &{:frame, null_topics_metadata_frame(&1)})
    watch_close(host)
    existing = supervisor_processes()

    {result, _log} =
      with_log(fn ->
        Client.start_link(
          [
            uris: [{host, 9092}, {host, 9093}],
            consumer_group: :no_consumer_group,
            ssl_options: FakeTransport.ssl_options(fake)
          ],
          :no_name
        )
      end)

    assert {:error, _} = result
    assert_receive {:pin_closed, %{port: 9092, reason: :init_error}}, 5_000
    refute_receive {:pin_closed, _}, 100

    connections = fake |> FakeTransport.connects() |> Enum.map(& &1.owner)
    assert connections != []
    refute Enum.any?(connections, &Process.alive?/1)
    assert supervisor_processes() -- existing == []
  end

  test "a request reads no connection setting from the application environment" do
    %{fake: fake, client: client} = start_pinned()
    FakeTransport.respond(fake, :produce, produce_reply())
    {:ok, _} = API.produce_one(client, "pin", 0, "v")
    :erlang.trace_pattern({:application, :get_env, 3}, true, [:global])
    :erlang.trace(client, true, [:call, {:tracer, self()}])
    on_exit(fn -> :erlang.trace_pattern({:application, :get_env, 3}, false, [:global]) end)

    {:ok, _} = API.produce_one(client, "pin", 0, "v")

    ref = :erlang.trace_delivered(client)
    assert_receive {:trace_delivered, ^client, ^ref}, 1_000

    refute_received {:trace, ^client, :call, {:application, :get_env, [:kafka_ex, :connect_timeout, _]}}
    refute_received {:trace, ^client, :call, {:application, :get_env, [:kafka_ex, :connection, _]}}
  end

  test "a stop that runs out of time leaves the process linked to the client supervisor alive" do
    {:ok, supervisor} = ClientSupervisor.start_link()

    assert %{active: 0, supervisors: 0} =
             DynamicSupervisor.count_children(ClientSupervisor.connection_supervisor(supervisor))

    start_stuck_child(supervisor)
    monitor = Process.monitor(supervisor)

    assert ClientSupervisor.stop(supervisor, :shutdown, 100) == :ok
    assert_receive {:DOWN, ^monitor, :process, ^supervisor, :killed}, 1_000
  end

  defp start_stuck_child(supervisor) do
    test = self()

    stuck = fn ->
      Process.flag(:trap_exit, true)
      send(test, :trapping)

      receive do
        :never -> :ok
      end
    end

    {:ok, _pid} =
      DynamicSupervisor.start_child(
        ClientSupervisor.connection_supervisor(supervisor),
        %{id: :stuck, start: {Task, :start_link, [stuck]}, shutdown: 5_000}
      )

    assert_receive :trapping, 1_000
  end

  test "a connection is a temporary child with a 2 second shutdown" do
    assert %{restart: :temporary, shutdown: 2_000, start: {ConnectionStateMachine, :start_link, [_]}} =
             Supervisor.child_spec({ConnectionStateMachine, %{}}, [])
  end

  describe "a running client" do
    setup do
      pinned = start_pinned()
      FakeTransport.respond(pinned.fake, :produce, produce_reply())
      watch_close(pinned.host)
      Map.put(pinned, :supervisor, :sys.get_state(pinned.client).supervisor)
    end

    test "is linked to its supervisor and does not trap exits", %{client: client, supervisor: supervisor} do
      assert {:links, links} = Process.info(client, :links)
      assert supervisor in links
      assert Process.info(client, :trap_exit) == {:trap_exit, false}
    end

    test "stopped, closes every connection with :shutdown and leaves no supervisor or connection alive", ctx do
      assert_teardown(ctx, fn client ->
        :ok = GenServer.stop(client, :normal, 5_000)
        refute Process.alive?(ctx.supervisor)
      end)
    end

    test "killed, closes every connection with :shutdown and leaves no supervisor or connection alive", ctx do
      assert_teardown(ctx, &Process.exit(&1, :kill))
    end

    test "lists its metadata store, connection supervisor and connection registry, in reverse start order", %{
      supervisor: supervisor
    } do
      assert [
               {:metadata_store, store, :worker, _},
               {:connections, connections, :supervisor, _},
               {:connection_registry, registry, :worker, _}
             ] = Supervisor.which_children(supervisor)

      assert is_pid(store) and is_pid(connections) and is_pid(registry)
    end

    test "stopped, takes its metadata store down and closes the store's metadata connection with :shutdown", %{
      client: client,
      supervisor: supervisor,
      host: host
    } do
      store = ClientSupervisor.metadata_store(supervisor)
      {:ok, metadata} = MetadataStore.connection(store, {host, 9092}, 1_000)
      assert connections(supervisor) == [metadata]
      store_ref = Process.monitor(store)
      metadata_ref = Process.monitor(metadata)

      :ok = GenServer.stop(client, :normal, 5_000)

      assert_receive {:DOWN, ^store_ref, :process, ^store, :shutdown}, 5_000
      assert_receive {:DOWN, ^metadata_ref, :process, ^metadata, _reason}, 5_000
      assert_receive {:pin_closed, %{port: 9092, reason: :shutdown}}, 1_000
      refute_receive {:pin_closed, _}, 100
    end

    test "survives its connection supervisor being killed, which the client supervisor restarts", %{
      client: client,
      supervisor: supervisor
    } do
      connection_supervisor = ClientSupervisor.connection_supervisor(supervisor)
      refs = Enum.map([connection_supervisor | connections(supervisor)], &Process.monitor/1)

      Process.exit(connection_supervisor, :kill)

      for ref <- refs, do: assert_receive({:DOWN, ^ref, :process, _pid, _reason}, 5_000)
      restarted = ClientSupervisor.connection_supervisor(supervisor)
      assert is_pid(restarted) and restarted != connection_supervisor
      assert Process.alive?(client)
      assert {:ok, %RecordMetadata{base_offset: 7}} = API.produce(client, @topic, 0, [%{value: "v"}])
    end

    test "answers without a connection while its connection supervisor is down, and connects once it is back", %{
      client: client,
      supervisor: supervisor
    } do
      :ok = Supervisor.terminate_child(supervisor, :connections)
      assert ClientSupervisor.connection_supervisor(supervisor) == nil
      {:ok, before} = API.correlation_id(client)
      attach_request_start()

      {result, _log} = with_log(fn -> API.produce(client, @topic, 0, [%{value: "v"}]) end)

      assert result == {:error, :no_broker}
      assert Process.alive?(client)
      assert API.correlation_id(client) == {:ok, before}
      refute_received {:request_start, _metadata}

      {:ok, _supervisor} = Supervisor.restart_child(supervisor, :connections)
      assert {:ok, %RecordMetadata{base_offset: 7}} = API.produce(client, @topic, 0, [%{value: "v"}])
    end

    test "crashes its connection registry when a connection fails to start", %{client: client, supervisor: supervisor} do
      registry = ClientSupervisor.connection_registry(supervisor)
      registry_ref = Process.monitor(registry)
      fail_connection_start()

      {exit, _log} = with_log(fn -> catch_exit(API.produce(client, @topic, 0, [%{value: "v"}])) end)

      assert_receive {:DOWN, ^registry_ref, :process, ^registry, {{:case_clause, {:error, _}}, [_ | _]} = crash}, 5_000
      assert {^crash, {GenServer, :call, [^registry, {:connect, _key}, _timeout]}} = exit
      assert Process.alive?(client)
    end

    test "crashes its connection registry when its connection supervisor exits on a start", %{
      client: client,
      supervisor: supervisor
    } do
      registry = ClientSupervisor.connection_registry(supervisor)
      registry_ref = Process.monitor(registry)
      :sys.replace_state(ClientSupervisor.connection_supervisor(supervisor), fn _state -> :broken end)

      {exit, _log} = with_log(fn -> catch_exit(API.produce(client, @topic, 0, [%{value: "v"}])) end)

      assert_receive {:DOWN, ^registry_ref, :process, ^registry,
                      {_crash, {GenServer, :call, [_supervisor, {:start_child, _child}, :infinity]}} = crash},
                     5_000

      assert {^crash, {GenServer, :call, [^registry, {:connect, _key}, _timeout]}} = exit
      assert Process.alive?(client)
    end
  end

  test "a failed boot leaves no supervisor or connection alive" do
    with_env(:sleep_for_reconnect, 0)
    Process.flag(:trap_exit, true)
    %{fake: fake, host: host} = start_fake([{1, 9092}])
    before = supervisor_processes()

    {result, _log} =
      with_log(fn ->
        Client.start_link(
          [
            uris: [{host, 9999}],
            consumer_group: :no_consumer_group,
            ssl_options: FakeTransport.ssl_options(fake)
          ],
          :no_name
        )
      end)

    assert {:error, _reason} = result
    assert [_ | _] = FakeTransport.connects(fake)
    assert supervisor_processes() -- before == []

    %{client: client} = start_pinned()
    assert client |> :sys.get_state() |> Map.fetch!(:supervisor) |> connections() |> length() == 1
    assert length(supervisor_processes() -- before) == 3
  end

  test "a boot every bootstrap broker refuses leaves no metadata store alive" do
    sleep = Application.get_env(:kafka_ex, :sleep_for_reconnect)
    Application.put_env(:kafka_ex, :sleep_for_reconnect, 0)
    on_exit(fn -> Application.put_env(:kafka_ex, :sleep_for_reconnect, sleep) end)
    %{client: client, fake: fake, host: host} = start_pinned()
    store = client |> :sys.get_state() |> Map.fetch!(:supervisor) |> ClientSupervisor.metadata_store()
    before = store_processes()
    assert store in before
    Process.flag(:trap_exit, true)
    gate = PinGate.new()
    PinGate.close(gate)

    opts = [
      uris: [{host, 9092}],
      consumer_group: :no_consumer_group,
      ssl_options: PinGate.ssl_options(gate, fake)
    ]

    {result, _log} = with_log(fn -> Client.start_link(opts, :no_name) end)

    assert {:error, _reason} = result
    assert store_processes() -- before == []
  end

  defp assert_teardown(%{client: client, supervisor: supervisor}, stop) do
    {:ok, _} = API.produce(client, @topic, 0, [%{value: "v"}])
    {:ok, _} = API.produce(client, @topic, 1, [%{value: "v"}])
    processes = [supervisor | connections(supervisor)]
    assert length(processes) == 4
    refs = Enum.map(processes, &Process.monitor/1)

    stop.(client)

    for ref <- refs, do: assert_receive({:DOWN, ^ref, :process, _pid, _reason}, 5_000)
    for _connection <- 1..3, do: assert_receive({:pin_closed, %{reason: :shutdown}}, 1_000)
    refute_receive {:pin_closed, _}, 100
  end

  defp attach_request_start do
    forward_telemetry([[:kafka_ex, :request, :start]], fn _event, _measurements, metadata ->
      {:request_start, metadata}
    end)
  end

  defp connections(supervisor) do
    for {_id, pid, _type, _modules} <-
          DynamicSupervisor.which_children(ClientSupervisor.connection_supervisor(supervisor)),
        is_pid(pid) do
      pid
    end
  end

  defp store_processes do
    for pid <- Process.list(),
        {:dictionary, dictionary} <- [Process.info(pid, :dictionary)],
        dictionary[:"$initial_call"] == {MetadataStore, :init, 1} do
      pid
    end
  end
end
