defmodule KafkaEx.Client.NoBrokerReasonTest do
  @moduledoc """
  `:no_broker` collapses three different causes — unknown topic, unknown
  partition, unknown node — so the reason has to reach the log, otherwise a
  leaderless partition is indistinguishable from a deleted topic.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias KafkaEx.Client
  alias KafkaEx.Client.NodeSelector
  alias KafkaEx.Client.State
  alias KafkaEx.Cluster.ClusterMetadata
  alias KafkaEx.Cluster.Topic

  defp state_with(topics) do
    %State{cluster_metadata: %ClusterMetadata{brokers: %{}, topics: topics}}
  end

  defp fetch_from(state, topic, partition) do
    request = %Kayrock.Fetch.V0.Request{replica_id: -1, max_wait_time: 1, min_bytes: 1, topics: []}
    selector = NodeSelector.topic_partition(topic, partition)

    capture_log(fn ->
      {:reply, reply, _} = Client.handle_call({:network_request, request, selector}, self(), state)
      send(self(), {:reply, reply})
    end)
  end

  test "names the missing partition behind :no_broker" do
    state = state_with(%{"t" => %Topic{name: "t", partition_leaders: %{0 => 1}}})

    log = fetch_from(state, "t", 7)

    assert_received {:reply, {:error, :no_broker}}
    assert log =~ "No broker for t/7"
    assert log =~ ":no_such_partition"
  end

  test "names the missing topic behind :no_broker" do
    log = fetch_from(state_with(%{}), "gone", 0)

    assert_received {:reply, {:error, :no_broker}}
    assert log =~ "No broker for gone/0"
    assert log =~ ":no_such_topic"
  end
end
