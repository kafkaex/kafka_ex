defmodule KafkaEx.Client.StoreWaitBudgetTest do
  use KafkaEx.TestSupport.PinCase, async: false

  alias KafkaEx.Client.Error
  alias KafkaEx.Client.NodeSelector
  alias KafkaEx.Client.RequestBudget
  alias KafkaEx.Client.RequestBuilder
  alias KafkaEx.Client.State
  alias KafkaEx.Cluster.ClusterMetadata
  alias KafkaEx.Config

  @not_leader_for_partition 6
  # Spent while the fetch waits on its connection's first connect, so only the resume can observe it.
  @budget_spent_while_parked_ms 300

  test "a send_request timeout below the default budget answers :timeout instead of exiting the caller" do
    %{client: client} = start_pinned()
    {:ok, request} = RequestBuilder.build(:metadata, [topics: [@topic]], %State{api_versions: versions(client)})
    store = suspend_store(client)

    {result, _log} = with_log(fn -> Client.send_request(client, request, NodeSelector.first_available(), 1_500) end)

    :sys.resume(store)
    assert result == {:error, :timeout}
    assert Process.alive?(client)
  end

  test "a call queued behind another answers :timeout within its own budget instead of exiting the caller" do
    with_request_timeout(100)
    %{client: client} = start_pinned()
    store = suspend_store(client)

    calls =
      for topic <- ["ghost-1", "ghost-2"] do
        Task.async(fn -> with_log(fn -> API.topics_metadata(client, [topic]) end) end)
      end

    results = Enum.map(calls, fn call -> call |> Task.await(15_000) |> elem(0) end)

    :sys.resume(store)
    assert results == [{:error, :timeout}, {:error, :timeout}]
    assert Process.alive?(client)
  end

  test "a cluster_metadata queued behind another call answers its last consistent snapshot instead of exiting the caller" do
    with_request_timeout(100)
    %{client: client} = start_pinned()
    {:ok, %ClusterMetadata{brokers: brokers} = last} = API.cluster_metadata(client)
    assert map_size(brokers) == 2
    store = suspend_store(client)

    blocking = Task.async(fn -> with_log(fn -> API.topics_metadata(client, ["ghost"]) end) end)
    await_store_call(store, 100)
    queued = Task.async(fn -> API.cluster_metadata(client) end)

    {blocked, _log} = Task.await(blocking, 15_000)
    view = Task.await(queued, 15_000)

    :sys.resume(store)
    assert blocked == {:error, :timeout}
    assert view == {:ok, last}
    assert Process.alive?(client)
  end

  test "an old-shape call waits on the store until one reply margin before the budget KafkaEx.API would give it" do
    with_request_timeout(100)
    %{client: client} = start_pinned()
    call_timeout = RequestBudget.call_budget(Config.request_timeout(), Client.retry_count())
    store = suspend_store(client)
    started = System.monotonic_time(:millisecond)

    {result, _log} = with_log(fn -> GenServer.call(client, {:topic_metadata, ["ghost"], false}, call_timeout) end)

    waited = System.monotonic_time(:millisecond) - started
    :sys.resume(store)
    assert result == {:error, :timeout}
    assert waited >= RequestBudget.deadline_budget(call_timeout)
  end

  test "a parked first fetch resumes under its caller's send-time budget, as a call taken from the mailbox would" do
    %{fake: fake, client: client} = start_pinned()
    FakeTransport.respond(fake, :fetch, &{:frame, not_leader_fetch_frame(&1)})
    FakeTransport.hold(fake, :api_versions)
    budget = {@budget_spent_while_parked_ms, System.monotonic_time(:millisecond)}

    parked = Task.async(fn -> with_log(fn -> fetch_with_budget(client, budget) end) end)
    wait_until(fn -> map_size(:sys.get_state(client).parked) == 1 end, 200)
    refute_receive :never, @budget_spent_while_parked_ms + 100
    refreshes = count(fake, :metadata)
    FakeTransport.release(fake, :api_versions)
    {resumed, _log} = Task.await(parked, 15_000)

    {from_mailbox, _log} = with_log(fn -> fetch_with_budget(client, budget) end)
    assert resumed == {:error, Error.build(:timeout, %{})}
    assert resumed == from_mailbox
    assert count(fake, :metadata) == refreshes
  end

  test "an offset_commit whose budget is spent while the store is detached still ends its commit span with the error" do
    %{client: client} = start_pinned()
    events = for phase <- [:start, :stop], do: [:kafka_ex, :consumer, :commit, phase]
    forward_telemetry(events, fn [_, _, _, phase], _measurements, metadata -> {:commit, phase, metadata} end)
    %{store: %{pid: store}} = :sys.get_state(client)
    :ok = :sys.suspend(client)
    ref = Process.monitor(store)
    Process.exit(store, :kill)
    assert_receive {:DOWN, ^ref, :process, ^store, :killed}, 1_000

    spent = [call_budget: {1, System.monotonic_time(:millisecond) - 1_000}]
    call = :gen_server.send_request(client, {:offset_commit, "g", [{@topic, [%{partition_num: 0, offset: 5}]}], spent})
    :ok = :sys.resume(client)

    assert {:reply, {:error, %Error{error: :timeout}}} = :gen_server.receive_response(call, 5_000)
    assert_receive {:commit, :start, _metadata}, 1_000
    assert_receive {:commit, :stop, %{result: :error}}, 1_000
  end

  defp versions(client), do: :sys.get_state(client).api_versions

  # The blocking call has reached the client only once its request waits in the suspended store's mailbox.
  defp await_store_call(store, tries) do
    case Process.info(store, :message_queue_len) do
      {:message_queue_len, 0} when tries > 0 ->
        refute_receive :never, 10
        await_store_call(store, tries - 1)

      {:message_queue_len, waiting} ->
        assert waiting > 0
    end
  end

  defp suspend_store(client) do
    store = :sys.get_state(client).store.pid
    :ok = :sys.suspend(store)
    store
  end

  defp fetch_with_budget(client, budget) do
    GenServer.call(client, {:fetch, @topic, 0, 0, [call_budget: budget]}, 10_000)
  end

  defp not_leader_fetch_frame(%{api_version: 0, correlation_id: corr}) do
    <<corr::32-signed, 1::32-signed, byte_size(@topic)::16-signed, @topic::binary, 1::32-signed, 0::32-signed,
      @not_leader_for_partition::16-signed, -1::64-signed, 0::32-signed>>
  end

  defp wait_until(condition, tries) do
    cond do
      condition.() ->
        :ok

      tries > 0 ->
        refute_receive :never, 10
        wait_until(condition, tries - 1)

      true ->
        flunk("condition not met")
    end
  end
end
