defmodule KafkaEx.Client.CallerLoopTest do
  use KafkaEx.TestSupport.PinCase, async: false

  alias KafkaEx.Client.Error

  @moduletag pin_set: :invariant

  @produce {:produce, "pin", 0, [%{value: "v"}], []}

  test "with no metadata table until the deadline, a request, cluster_metadata and list_groups answer :timeout" do
    with_request_timeout(100)
    %{client: client} = start_pinned()
    :ok = Supervisor.terminate_child(:sys.get_state(client).supervisor, :metadata_store)

    {results, _log} =
      with_log(fn ->
        {Client.request(client, @produce, 300), Client.request(client, {:cluster_metadata, []}, 300),
         API.cluster_metadata(client), API.list_groups(client)}
      end)

    assert {{:error, %Error{error: :timeout}}, {:error, :timeout}, {:error, :timeout}, {:error, :timeout}} = results
    assert Process.alive?(client)
  end

  test "on a :serial client, producers held at the broker reach it one at a time in the order they asked" do
    %{fake: fake, client: client} = start_pinned(client: [request_concurrency: :serial])
    FakeTransport.respond(fake, :produce, produce_reply())
    :ok = FakeTransport.subscribe(fake)
    FakeTransport.hold(fake, :produce)

    first = Task.async(fn -> API.produce(client, @topic, 0, [%{value: "v"}]) end)
    assert_receive {:fake_transport, :request, %{api: :produce, node_id: 1}}, 5_000
    second = Task.async(fn -> API.produce(client, @topic, 1, [%{value: "v"}]) end)
    wait_until(fn -> :queue.len(:sys.get_state(client).waiting) == 1 end)
    third = Task.async(fn -> API.produce(client, @topic, 0, [%{value: "v"}]) end)
    wait_until(fn -> :queue.len(:sys.get_state(client).waiting) == 2 end)
    refute_receive {:fake_transport, :request, %{api: :produce}}, 200

    FakeTransport.release(fake, :produce)

    assert_receive {:fake_transport, :request, %{api: :produce, node_id: 2}}, 5_000
    assert_receive {:fake_transport, :request, %{api: :produce, node_id: 1}}, 5_000
    assert [{:ok, _}, {:ok, _}, {:ok, _}] = Task.await_many([first, second, third], 10_000)
  end

  test "a :serial client finishes a group request whose coordinator it must discover first" do
    %{fake: fake, client: client} = start_pinned(client: [request_concurrency: :serial])
    FakeTransport.respond(fake, :heartbeat, %{error_code: 0})

    assert {:ok, _} = API.heartbeat(client, "g", "m", 1)
    assert count(fake, :find_coordinator) == 1
  end

  test "a request that timed out leaves nothing in its caller's mailbox, not even the late reply" do
    %{fake: fake, client: client} = start_pinned()
    FakeTransport.respond(fake, :produce, produce_reply())
    :ok = FakeTransport.subscribe(fake)
    FakeTransport.hold(fake, :produce)
    test = self()

    caller =
      spawn(fn ->
        result = Client.request(client, @produce, 1_000)
        send(test, {:answered, result})
        receive do: (:released -> :ok)
        Process.sleep(200)
        send(test, {:mailbox, Process.info(self(), :messages)})
      end)

    assert_receive {:answered, {:error, %Error{error: :timeout}}}, 5_000
    FakeTransport.release(fake, :produce)
    assert_receive {:fake_transport, :request, %{api: :produce}}, 1_000
    send(caller, :released)
    assert_receive {:mailbox, {:messages, []}}, 1_000
  end

  test "a request with a budget shorter than the connection's reply margin is still written and answered" do
    %{fake: fake, client: client} = start_pinned()
    FakeTransport.respond(fake, :produce, produce_reply())

    assert {:ok, _} = Client.request(client, @produce, 300)
    assert count(fake, :produce) == 1
  end

  test "a stale context after a written attempt fails that attempt, never rebuilding the join with a new member id" do
    %{fake: fake, client: client} = start_pinned(api_versions: %{consumer_group_heartbeat: 1..1})
    :ok = FakeTransport.subscribe(fake)
    FakeTransport.hold(fake, :consumer_group_heartbeat)
    store = :sys.get_state(client).store.pid
    opts = [member_epoch: 0, subscribed_topic_names: [@topic], topic_partitions: [], rebalance_timeout_ms: 10_000]

    join = Task.async(fn -> with_log(fn -> API.consumer_group_heartbeat(client, "g", opts) end) end)
    assert_receive {:fake_transport, :request, %{api: :consumer_group_heartbeat}}, 5_000
    ref = Process.monitor(store)
    Process.exit(store, :kill)
    assert_receive {:DOWN, ^ref, :process, ^store, :killed}, 1_000
    FakeTransport.respond(fake, :consumer_group_heartbeat, :close)
    FakeTransport.release(fake, :consumer_group_heartbeat)

    assert {{:error, _reason}, _log} = Task.await(join, 15_000)
    assert [_one_member] = fake |> requests(:consumer_group_heartbeat) |> Enum.map(&member_id/1) |> Enum.uniq()
  end

  test "a request whose client does not hand out a context in time ends its operation span with :timeout" do
    %{client: client} = start_pinned()
    events = for phase <- [:stop, :exception], do: [:kafka_ex, :produce, phase]
    forward_telemetry(events, fn [_, _, phase], _measurements, metadata -> {:produce, phase, metadata} end)
    :ok = Supervisor.terminate_child(:sys.get_state(client).supervisor, :metadata_store)

    produce = Task.async(fn -> Client.request(client, @produce, 1_000) end)
    refute_receive :never, 30
    :ok = :sys.suspend(client)

    assert {:error, %Error{error: :timeout}} = Task.await(produce, 5_000)
    :ok = :sys.resume(client)
    assert_receive {:produce, :stop, %{result: :error, error: %Error{error: :timeout}}}, 1_000
    refute_received {:produce, :exception, _metadata}
  end

  test "a direct request call gets :unsupported_call and the client keeps serving requests" do
    %{fake: fake, client: client} = start_pinned()
    FakeTransport.respond(fake, :produce, produce_reply())

    {reply, log} = with_log(fn -> GenServer.call(client, @produce) end)

    assert reply == {:error, :unsupported_call}
    assert log =~ "does not serve"
    assert {:ok, _} = Client.request(client, @produce, 5_000)
  end

  test "a client on an unreachable node is called through :erpc and exits its caller, as a call to it always did" do
    remote = {:kafka_ex_client, :kafka_ex_remote@nohost}
    message = {:cluster_metadata, []}

    assert {:noconnection, {Client, :request, [^remote, ^message, 100]}} =
             catch_exit(Client.request(remote, message, 100))
  end

  defp wait_until(condition, tries \\ 200) do
    cond do
      condition.() -> :ok
      tries == 0 -> flunk("condition never held")
      true -> retry_after_pause(condition, tries)
    end
  end

  defp retry_after_pause(condition, tries) do
    Process.sleep(10)
    wait_until(condition, tries - 1)
  end
end
