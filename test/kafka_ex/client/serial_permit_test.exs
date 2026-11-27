defmodule KafkaEx.Client.SerialPermitTest do
  use KafkaEx.TestSupport.PinCase, async: false

  import KafkaEx.TestHelpers, only: [oauthbearer: 0, wait_for: 2]

  alias KafkaEx.Client.State

  @moduletag pin_set: :invariant

  setup do
    pinned = start_pinned(client: [request_concurrency: :serial, auth: oauthbearer()])
    FakeTransport.respond(pinned.fake, :produce, produce_reply())
    FakeTransport.subscribe(pinned.fake)
    pinned
  end

  test "permits go out in the order they were asked for, each after its predecessor's release", %{client: client} do
    :ok = GenServer.call(client, :permit)
    first = ask_permit(client, :first, 1)
    _second = ask_permit(client, :second, 2)
    refute_receive {:granted, _waiter}, 100

    GenServer.cast(client, {:release, self()})
    assert_receive {:granted, :first}, 1_000
    refute_receive {:granted, :second}, 100

    send(first, :release)
    assert_receive {:granted, :second}, 1_000
  end

  test "a killed holder lets the next waiter through", %{client: client} do
    holder = ask_permit(client, :holder, 0)
    assert_receive {:granted, :holder}, 1_000
    _next = ask_permit(client, :next, 1)

    Process.exit(holder, :kill)
    assert_receive {:granted, :next}, 1_000
  end

  test "a waiter whose wait timed out leaves the queue", %{client: client} do
    :ok = GenServer.call(client, :permit)
    produce = {:produce, @topic, 0, [%{value: "v"}], []}
    waiter = Task.async(fn -> Client.request(client, produce, 200) end)

    assert {:error, %KafkaEx.Client.Error{error: :timeout}} = Task.await(waiter)
    assert :queue.is_empty(:sys.get_state(client).waiting)

    GenServer.cast(client, {:release, self()})
    assert :ok = GenServer.call(client, :permit, 1_000)
    refute_received {:fake_transport, :request, %{api: :produce}}
  end

  test "a request is not sent while another process holds the permit, and releases it when done", %{client: client} do
    :ok = GenServer.call(client, :permit)
    produce = Task.async(fn -> API.produce(client, @topic, 0, [%{value: "v"}]) end)
    refute_receive {:fake_transport, :request, %{api: :produce}}, 200

    GenServer.cast(client, {:release, self()})
    assert_receive {:fake_transport, :request, %{api: :produce}}, 5_000
    assert {:ok, _metadata} = Task.await(produce)
    assert :ok = GenServer.call(client, :permit, 1_000)
  end

  test ":context answers the client's state without its credentials", %{client: client} do
    context = GenServer.call(client, :context)
    state = :sys.get_state(client)

    assert %State{ssl_options: [], auth: nil, supervisor: nil, request_concurrency: :serial} = context
    assert state.ssl_options != [] and state.auth != nil
    refute Map.has_key?(context, :secrets)
    assert context.store == state.store
  end

  @tag capture_log: true
  test "a client started with an unknown request_concurrency fails with an ArgumentError naming the value" do
    Process.flag(:trap_exit, true)
    args = [uris: [], consumer_group: :no_consumer_group, request_concurrency: :seriall]

    assert {:error, {%ArgumentError{message: message}, _stacktrace}} = Client.start_link(args, :no_name)
    assert message =~ ":seriall"
  end

  defp ask_permit(client, name, queued) do
    test = self()

    waiter =
      spawn(fn ->
        :ok = GenServer.call(client, :permit)
        send(test, {:granted, name})

        receive do
          :release -> GenServer.cast(client, {:release, self()})
        end
      end)

    wait_for(fn -> :queue.len(:sys.get_state(client).waiting) == queued end, 10)
    waiter
  end
end
