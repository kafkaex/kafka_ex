defmodule KafkaEx.API.CallBudgetContractTest do
  use ExUnit.Case, async: false
  use Mimic

  alias KafkaEx.API
  alias KafkaEx.Client
  alias KafkaEx.Client.NodeSelector
  alias KafkaEx.Client.RequestBudget
  alias KafkaEx.Cluster.Broker
  alias KafkaEx.Cluster.ClusterMetadata
  alias KafkaEx.Config

  defmodule Recorder do
    use GenServer

    def start_link(test), do: GenServer.start_link(__MODULE__, test)

    @impl true
    def init(test), do: {:ok, test}

    @impl true
    def handle_call({:cluster_metadata, _opts} = message, _from, test) do
      send(test, {:recorded, message})
      {:reply, {:ok, %ClusterMetadata{brokers: %{1 => %Broker{node_id: 1, host: "h", port: 9092}}}}, test}
    end

    def handle_call(message, _from, test) do
      send(test, {:recorded, message})
      {:reply, {:error, :recorded}, test}
    end
  end

  setup do
    {:ok, recorder: start_supervised!({Recorder, self()})}
  end

  defp recorded(call) do
    try do
      call.()
    rescue
      _error -> :ok
    catch
      :exit, _reason -> :ok
    end

    assert_receive {:recorded, message}, 1_000
    message
  end

  defp budget_of(message) do
    opts =
      case message do
        {:metadata, _topics, opts, _api_version} -> opts
        _other -> elem(message, tuple_size(message) - 1)
      end

    Keyword.fetch!(opts, :call_budget)
  end

  defp assert_budget(message, call_timeout) do
    assert {ms, sent_at} = budget_of(message)
    assert ms == RequestBudget.deadline_budget(call_timeout)
    assert sent_at <= System.monotonic_time(:millisecond)
  end

  defp generic, do: RequestBudget.call_budget(Config.request_timeout(), Client.retry_count())

  # The original still reaches the Recorder, so a test sees both the message and the timeout it was sent with.
  defp stub_request do
    test = self()

    stub(Client, :request, fn client, message, timeout ->
      send(test, {:requested, message, timeout})
      call_original(Client, :request, [client, message, timeout])
    end)
  end

  defp requested_timeout(call) do
    message = recorded(call)
    assert_receive {:requested, ^message, timeout}, 1_000
    timeout
  end

  test "every KafkaEx.API call that reaches the network passes its call budget as the request timeout", %{
    recorder: rec
  } do
    stub_request()
    n = Client.retry_count()
    coordinator = Client.coordinator_max_attempts()

    rows = [
      {fn -> API.list_offsets(rec, [{"t", [%{partition_num: 0, timestamp: -1}]}]) end, generic()},
      {fn -> API.cluster_metadata(rec) end, generic()},
      {fn -> API.metadata(rec) end, generic()},
      {fn -> API.metadata(rec, ["t"], []) end, generic()},
      {fn -> API.api_versions(rec) end, generic()},
      {fn -> API.describe_group(rec, "g") end, generic()},
      {fn -> API.leave_group(rec, "g", "m") end, generic()},
      {fn -> API.find_coordinator(rec, "g") end, generic()},
      {fn -> API.describe_consumer_group(rec, "g") end, generic()},
      {fn -> API.fetch_committed_offset(rec, "g", "t", [%{partition_num: 0}]) end, generic()},
      {fn -> API.commit_offset(rec, "g", "t", [%{partition_num: 0, offset: 1}]) end, generic()},
      {fn -> API.produce(rec, "t", 0, [%{value: "v"}]) end, generic()},
      {fn -> API.heartbeat(rec, "g", "m", 1, network_timeout: 5_000) end, RequestBudget.call_budget(5_000, n)},
      {fn -> API.consumer_group_heartbeat(rec, "g", network_timeout: 3_000) end, RequestBudget.call_budget(3_000, n)},
      {fn -> API.join_group(rec, "g", "") end, RequestBudget.call_budget(65_000, coordinator)},
      {fn -> API.sync_group(rec, "g", 1, "m") end, RequestBudget.call_budget(35_000, coordinator)},
      {fn -> API.fetch(rec, "t", 0, 0) end, RequestBudget.call_budget(15_000, n)},
      {fn -> API.create_topics(rec, [], 10_000) end, RequestBudget.call_budget(15_000, n)},
      {fn -> API.delete_topics(rec, [], 30_000) end, RequestBudget.call_budget(35_000, n)}
    ]

    for {call, call_timeout} <- rows do
      assert requested_timeout(call) == call_timeout
    end
  end

  test "list_groups asks each node with the generic timeout", %{recorder: rec} do
    stub_request()
    generic = generic()

    assert {:cluster_metadata, _opts} = recorded(fn -> API.list_groups(rec) end)
    assert_receive {:requested, {:cluster_metadata, _opts}, ^generic}, 1_000
    assert_receive {:requested, {:list_groups, 1, _opts}, ^generic}, 1_000
  end

  test "topics_metadata sends the keyword shape", %{recorder: rec} do
    stub_request()
    message = recorded(fn -> API.topics_metadata(rec, ["t"]) end)

    assert {:topic_metadata, ["t"], opts} = message
    assert opts[:allow_topic_creation] == false
    assert_receive {:requested, ^message, timeout}, 1_000
    assert timeout == generic()
  end

  test "send_request sends the four-tuple shape with its own timeout's budget", %{recorder: rec} do
    request = %Kayrock.Metadata.V0.Request{topics: []}

    message = recorded(fn -> Client.send_request(rec, request, NodeSelector.first_available()) end)
    assert {:network_request, ^request, %NodeSelector{}, _opts} = message
    assert_budget(message, RequestBudget.call_budget(Config.request_timeout()))

    message = recorded(fn -> Client.send_request(rec, request, NodeSelector.first_available(), 1_500) end)
    assert {750, _sent_at} = budget_of(message)
  end

  test "calls that never wait send their messages unchanged", %{recorder: rec} do
    assert recorded(fn -> API.correlation_id(rec) end) == :correlation_id

    assert recorded(fn -> API.set_consumer_group_for_auto_commit(rec, "g") end) ==
             {:set_consumer_group_for_auto_commit, "g"}
  end
end
