defmodule KafkaEx.Client.StoreWaitBudgetTest do
  use KafkaEx.TestSupport.PinCase, async: false

  alias KafkaEx.Client.Error
  alias KafkaEx.Client.NodeSelector
  alias KafkaEx.Client.RequestBuilder
  alias KafkaEx.Client.State

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

  test "a cluster_metadata whose store does not answer within its budget answers :timeout instead of exiting the caller" do
    with_request_timeout(100)
    %{client: client} = start_pinned()
    store = suspend_store(client)

    {view, _log} = with_log(fn -> API.cluster_metadata(client) end)

    :sys.resume(store)
    assert view == {:error, :timeout}
    assert Process.alive?(client)
  end

  test "an offset_commit whose budget runs out while the store does not answer still ends its commit span with the error" do
    %{client: client} = start_pinned()
    events = for phase <- [:start, :stop], do: [:kafka_ex, :consumer, :commit, phase]
    forward_telemetry(events, fn [_, _, _, phase], _measurements, metadata -> {:commit, phase, metadata} end)
    store = suspend_store(client)
    commit = {:offset_commit, "g", [{@topic, [%{partition_num: 0, offset: 5}]}], []}

    {result, _log} = with_log(fn -> Client.request(client, commit, 300) end)

    :sys.resume(store)
    assert {:error, %Error{error: :timeout}} = result
    assert_receive {:commit, :start, _metadata}, 1_000
    assert_receive {:commit, :stop, %{result: :error}}, 1_000
  end

  defp versions(client), do: :sys.get_state(client).api_versions

  defp suspend_store(client) do
    store = :sys.get_state(client).store.pid
    :ok = :sys.suspend(store)
    store
  end
end
