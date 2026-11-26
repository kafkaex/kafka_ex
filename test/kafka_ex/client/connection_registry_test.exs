defmodule KafkaEx.Client.ConnectionRegistryTest do
  use KafkaEx.TestSupport.PinCase, async: false

  alias KafkaEx.Client.ConnectionRegistry
  alias KafkaEx.Client.Supervisor, as: ClientSupervisor
  alias KafkaEx.Messages.RecordMetadata

  @moduletag pin_set: :invariant
  @moduletag capture_log: true

  @idle_timeout 300

  setup do
    pinned = start_pinned(connection: [idle_timeout: @idle_timeout])
    supervisor = :sys.get_state(pinned.client).supervisor
    registry = ClientSupervisor.connection_registry(supervisor)
    Map.merge(pinned, %{supervisor: supervisor, registry: registry, table: :sys.get_state(pinned.client).conns})
  end

  test "two callers asking at once for the same connection get one pid and one row", ctx do
    key = {ctx.host, 9093, :data}
    :ok = :sys.suspend(ctx.registry)
    callers = for _caller <- 1..2, do: Task.async(fn -> ConnectionRegistry.connect(ctx.registry, key, 5_000) end)
    wait_for(fn -> Process.info(ctx.registry, :message_queue_len) == {:message_queue_len, 2} end)
    :ok = :sys.resume(ctx.registry)

    assert [{:ok, pid}, {:ok, pid}] = Task.await_many(callers)
    assert :ets.lookup(ctx.table, key) == [{key, pid}]
  end

  test "a killed connection leaves the table and the next connect starts a new one", ctx do
    key = {ctx.host, 9093, :data}
    {:ok, old} = ConnectionRegistry.connect(ctx.registry, key, 5_000)
    ref = Process.monitor(old)

    Process.exit(old, :kill)

    assert_receive {:DOWN, ^ref, :process, ^old, :killed}, 1_000
    wait_for(fn -> :ets.lookup(ctx.table, key) == [] end)
    assert {:ok, new} = ConnectionRegistry.connect(ctx.registry, key, 5_000)
    assert new != old and Process.alive?(new)
  end

  test "a killed registry restarts the connections and the store, and the next produce succeeds", ctx do
    FakeTransport.respond(ctx.fake, :produce, produce_reply())
    {:ok, _} = API.produce(ctx.client, @topic, 0, [%{value: "v"}])
    [{_key, connection}] = :ets.lookup(ctx.table, {ctx.host, 9092, :data})
    store = ClientSupervisor.metadata_store(ctx.supervisor)
    refs = Enum.map([ctx.registry, connection, store], &Process.monitor/1)

    Process.exit(ctx.registry, :kill)

    for ref <- refs, do: assert_receive({:DOWN, ^ref, :process, _pid, _reason}, 5_000)
    wait_for(fn -> ClientSupervisor.metadata_store(ctx.supervisor) not in [nil, store] end)
    assert ClientSupervisor.connection_registry(ctx.supervisor) not in [nil, ctx.registry]
    assert {:ok, %RecordMetadata{base_offset: 7}} = API.produce(ctx.client, @topic, 0, [%{value: "v"}])
    assert Process.alive?(ctx.client)
  end

  test "a fetch left in an idle connection's mailbox succeeds on a new connection without a retry", ctx do
    FakeTransport.respond(ctx.fake, :fetch, &{:frame, fetch_frame(&1, -1)})
    {:ok, _} = API.fetch(ctx.client, @topic, 0, 0, max_wait_time: 10)
    key = {ctx.host, 9092, {:fetch, @topic, 0}}
    [{^key, conn}] = :ets.lookup(ctx.table, key)
    ref = Process.monitor(conn)

    :ok = :sys.suspend(conn)
    Process.sleep(round(@idle_timeout * 1.2) + 50)
    {:message_queue_len, timers} = Process.info(conn, :message_queue_len)
    caller = Task.async(fn -> with_log(fn -> API.fetch(ctx.client, @topic, 0, 0, max_wait_time: 10) end) end)
    wait_for(fn -> Process.info(conn, :message_queue_len) == {:message_queue_len, timers + 1} end)
    :ok = :sys.resume(conn)

    assert {{:ok, _fetch}, log} = Task.await(caller)
    refute log =~ "failed"
    assert_receive {:DOWN, ^ref, :process, ^conn, :normal}, 1_000
    assert [{^key, new}] = :ets.lookup(ctx.table, key)
    assert new != conn
    assert length(requests(ctx.fake, :fetch)) == 2
  end

  defp wait_for(condition), do: KafkaEx.TestHelpers.wait_for_value(condition, & &1, 10, 300)
end
