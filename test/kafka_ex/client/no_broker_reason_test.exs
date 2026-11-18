defmodule KafkaEx.Client.NoBrokerReasonTest do
  @moduledoc """
  `:no_broker` collapses three different causes — unknown topic, unknown
  partition, unknown node — so the reason has to reach the log, otherwise a
  leaderless partition is indistinguishable from a deleted topic.
  """
  use KafkaEx.TestSupport.PinCase, async: false

  alias KafkaEx.Client.NodeSelector

  @moduletag pin_set: :invariant

  setup do
    {:ok, start_pinned(topics: %{"t" => [1]})}
  end

  defp fetch_from(client, topic, partition) do
    request = %Kayrock.Fetch.V0.Request{replica_id: -1, max_wait_time: 1, min_bytes: 1, topics: []}
    selector = NodeSelector.topic_partition(topic, partition)

    capture_log(fn ->
      send(self(), {:reply, Client.send_request(client, request, selector)})
    end)
  end

  test "names the missing partition behind :no_broker", %{client: client} do
    log = fetch_from(client, "t", 7)

    assert_received {:reply, {:error, :no_broker}}
    assert log =~ "No broker for t/7"
    assert log =~ ":no_such_partition"
  end

  test "names the missing topic behind :no_broker", %{client: client} do
    log = fetch_from(client, "gone", 0)

    assert_received {:reply, {:error, :no_broker}}
    assert log =~ "No broker for gone/0"
    assert log =~ ":no_such_topic"
  end
end
