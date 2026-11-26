defmodule KafkaEx.Pins.OrderingPinTest do
  use KafkaEx.TestSupport.PinCase, async: false

  @serial_window 200
  @concurrent_window 5_000

  setup tags do
    client = if tags[:serial_mode], do: [request_concurrency: :serial], else: []
    pinned = start_pinned(topics: %{@topic => [1, 1, 2]}, client: client)
    FakeTransport.respond(pinned.fake, :produce, produce_reply())
    FakeTransport.respond(pinned.fake, :heartbeat, %{error_code: 0})
    FakeTransport.respond(pinned.fake, :fetch, &{:frame, fetch_frame(&1, -1)})
    FakeTransport.subscribe(pinned.fake)
    pinned
  end

  defp hold_long_fetch(%{fake: fake, client: client}) do
    FakeTransport.hold(fake, :fetch)
    held = Task.async(fn -> API.fetch(client, @topic, 0, 0, max_wait_time: 10) end)
    assert_receive {:fake_transport, :request, %{api: :fetch, node_id: 1}}, 5_000
    held
  end

  @others [
    {"metadata", quote(do: %{api: :metadata}), quote(do: API.metadata(var!(client)))},
    {"a produce to another partition on the same broker", quote(do: %{api: :produce, node_id: 1}),
     quote(do: API.produce(var!(client), @topic, 1, [%{value: "v"}]))},
    {"a fetch of another partition on the same broker", quote(do: %{api: :fetch, node_id: 1}),
     quote(do: API.fetch(var!(client), @topic, 1, 0, max_wait_time: 10))},
    {"a produce to another broker", quote(do: %{api: :produce, node_id: 2}),
     quote(do: API.produce(var!(client), @topic, 2, [%{value: "v"}]))},
    {"a group heartbeat to a coordinator on another broker", quote(do: %{api: :heartbeat, node_id: 2}),
     quote(do: API.heartbeat(var!(client), "g", "m", 1))}
  ]

  describe "requests behind a long fetch on a :serial client" do
    @describetag pin_set: :invariant
    @describetag serial_mode: true

    for {name, request, call} <- @others do
      test "#{name} is not received until the fetch is answered", %{fake: fake, client: client} = ctx do
        held = hold_long_fetch(ctx)
        var!(client) = client
        other = Task.async(fn -> unquote(call) end)

        refute_receive {:fake_transport, :request, unquote(request)}, @serial_window
        FakeTransport.release(fake, :fetch)

        assert_receive {:fake_transport, :request, unquote(request)}, 5_000
        assert {:ok, _} = Task.await(held)
        assert {:ok, _} = Task.await(other)
      end
    end
  end

  describe "Goal 1: requests behind a long fetch" do
    @describetag pin_set: :target_state

    for {name, request, call} <- @others do
      test "#{name} is received while the fetch is held", %{fake: fake, client: client} = ctx do
        held = hold_long_fetch(ctx)
        var!(client) = client
        other = Task.async(fn -> unquote(call) end)

        assert_receive {:fake_transport, :request, unquote(request)}, @concurrent_window
        FakeTransport.release(fake, :fetch)
        assert {:ok, _} = Task.await(held)
        assert {:ok, _} = Task.await(other)
      end
    end
  end

  describe "KIP-848 shutdown order" do
    @describetag pin_set: :invariant

    test "a join from a caller killed before it was written never reaches the broker after the leave", %{
      fake: fake,
      client: client
    } do
      FakeTransport.respond(fake, :consumer_group_heartbeat, fn request ->
        epoch = member_epoch(request)
        {:frame, consumer_group_heartbeat_frame(request, "m", if(epoch < 0, do: epoch, else: epoch + 1))}
      end)

      FakeTransport.hold(fake, :find_coordinator)

      join =
        {:consumer_group_heartbeat, "g",
         [
           member_id: "m",
           member_epoch: 0,
           subscribed_topic_names: [@topic],
           topic_partitions: [],
           rebalance_timeout_ms: 10_000,
           network_timeout: 10_000
         ]}

      joiner = spawn(fn -> Client.request(client, join, 10_000) end)
      assert_receive {:fake_transport, :request, %{api: :find_coordinator}}, 5_000
      joiner_ref = Process.monitor(joiner)
      Process.exit(joiner, :kill)
      assert_receive {:DOWN, ^joiner_ref, :process, ^joiner, :killed}, 5_000
      leave = Task.async(fn -> API.consumer_group_heartbeat(client, "g", member_id: "m", member_epoch: -1) end)
      FakeTransport.release(fake, :find_coordinator)

      assert {:ok, _} = Task.await(leave)
      assert {:ok, _} = API.heartbeat(client, "g", "m", 1)
      assert fake |> requests(:consumer_group_heartbeat) |> Enum.map(&member_epoch/1) |> List.last() == -1
    end
  end
end
