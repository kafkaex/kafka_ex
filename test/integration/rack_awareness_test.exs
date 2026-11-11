defmodule KafkaEx.Integration.RackAwarenessTest do
  @moduledoc """
  End-to-end verification of KIP-392 (rack-aware fetching) against a
  rack-tagged cluster: the request carries `rack_id`, the response carries
  `preferred_read_replica`, the broker picks a same-rack replica, and the
  client caches the hint so subsequent fetches go to the preferred broker.

  Brokers are tagged in `docker-compose.yml` (`KAFKA_BROKER_RACK`) and the
  cluster sets `KAFKA_REPLICA_SELECTOR_CLASS=RackAwareReplicaSelector` in
  `docker-compose-kafka.env`.

  The suite creates the topic it fetches. The name does not matter. Two
  things otherwise make this flake in CI:

    * A Fetch for a missing topic auto-creates it with the broker defaults
      (`num.partitions=1`, replication factor 1).
    * Kafka picks partition leaders with a random starting replica, so a
      hardcoded "partition 0 lives in az-a" map does not survive a new
      cluster.
  """
  use ExUnit.Case, async: true
  import ExUnit.CaptureLog
  import KafkaEx.TestSupport.ProcessHelpers
  @moduletag :rack_awareness

  alias KafkaEx.API
  alias KafkaEx.Client
  alias KafkaEx.Client.NodeSelector
  alias KafkaEx.Cluster.ClusterMetadata
  alias KafkaEx.Cluster.PartitionInfo
  alias KafkaEx.Messages.CreateTopics
  alias KafkaEx.Messages.Fetch

  @topic "rack_aware_test"
  @partition_count 3
  @replication_factor 3
  @metadata_opts [api_version: 9]
  @topic_ready_timeout_ms 30_000

  # Broker id to rack, as set in docker-compose.yml. Stable, unlike leaders.
  @rack_to_broker_id %{"az-a" => 1, "az-b" => 2, "az-c" => 3}

  setup_all do
    pid = start_client!()

    try do
      ensure_replicated_topic!(pid)
      :ok
    after
      stop_safely(pid)
    end
  end

  setup do
    pid = start_client!()
    on_exit(fn -> stop_safely(pid) end)
    {:ok, %{client: pid}}
  end

  describe "broker metadata" do
    test "keeps track of broker-reported rack IDs", %{client: client} do
      cm = cluster_metadata!(client)

      for {expected_rack, broker_id} <- @rack_to_broker_id do
        broker = Map.fetch!(cm.brokers, broker_id)
        assert broker.rack == expected_rack
      end
    end
  end

  describe "Fetch V11 with rack_id (KIP-392)" do
    test "cross-rack request returns the replica in the client's rack", %{client: client} do
      cases = rack_cases(cluster_metadata!(client), &(&1 != -1))
      assert cases != [], "expected at least one cross-rack replica"

      # A fresh client each time. The first response is cached and the next
      # fetch is sent to that replica, which answers -1 ("stay here") instead
      # of naming the replica for a different rack.
      for {partition, client_rack, preferred} <- cases do
        with_fresh_client(fn fresh ->
          await_preferred_fetch(fresh, partition, client_rack, preferred)
        end)
      end
    end

    test "same-rack-as-leader request returns -1 (no redirect needed)", %{client: client} do
      cases = rack_cases(cluster_metadata!(client), &(&1 == -1))
      assert cases != [], "expected at least one partition whose leader is in a client rack"

      for {partition, client_rack, -1} <- cases do
        with_fresh_client(fn fresh ->
          assert {:ok, %Fetch{preferred_read_replica: -1}} =
                   API.fetch(fresh, @topic, partition, 0, rack_id: client_rack, min_bytes: 0)
        end)
      end
    end

    test "missing rack_id → no preference (-1)", %{client: client} do
      {:ok, %Fetch{} = result} = API.fetch(client, @topic, 0, 0, min_bytes: 0)
      assert result.preferred_read_replica in [-1, nil]
    end

    test "empty rack_id (default) → no preference (-1)", %{client: client} do
      {:ok, %Fetch{} = result} = API.fetch(client, @topic, 0, 0, rack_id: "", min_bytes: 0)
      assert result.preferred_read_replica in [-1, nil]
    end

    test "unknown rack_id → no preference (-1)", %{client: client} do
      {:ok, %Fetch{} = result} = API.fetch(client, @topic, 0, 0, rack_id: "az-unknown", min_bytes: 0)
      assert result.preferred_read_replica in [-1, nil]
    end
  end

  describe "preferred-replica cache must not leak into writes" do
    @partition 0

    setup %{client: client} do
      cm = cluster_metadata!(client)
      %PartitionInfo{leader: leader, replicas: replicas} = partition!(cm, @partition)
      follower = Enum.find(replicas, &(&1 != leader))
      cross_rack = rack!(cm, follower)

      {:ok, leader: leader, follower: follower, cross_rack: cross_rack}
    end

    test "the topic_partition selector still resolves to the leader after a fetch cached a follower",
         %{client: client, leader: leader, follower: follower, cross_rack: cross_rack} do
      await_preferred_fetch(client, @partition, cross_rack, follower)

      {:ok, cm} = API.cluster_metadata(client)
      assert ClusterMetadata.preferred_replica(cm, @topic, @partition) == follower

      # produce and list_offsets both select the node via this exact selector; it
      # must resolve to the leader, never the cached follower.
      assert ClusterMetadata.select_node(cm, NodeSelector.topic_partition(@topic, @partition)) ==
               {:ok, leader}
    end

    test "producing after a cross-rack fetch is not routed to the cached follower",
         %{client: client, follower: follower, cross_rack: cross_rack} do
      await_preferred_fetch(client, @partition, cross_rack, follower)

      # Produce recovers (the leadership error refreshes metadata, dropping the
      # cache, and the retry hits the leader), so the return value is {:ok, _}.
      # The bug is the wasted, failed first attempt against the follower,
      # `:not_leader_for_partition` or `:not_leader_or_follower` in the log.
      log =
        capture_log(fn ->
          assert {:ok, _} = API.produce(client, @topic, @partition, [%{value: "must-hit-leader"}])
        end)

      refute log =~ "not_leader",
             "producer was routed to the cached follower instead of the leader:\n#{log}"
    end
  end

  # -1 is not cached, so polling this client still reaches the leader until
  # the broker names a follower.
  defp await_preferred_fetch(client, partition, rack_id, expected) do
    wait_until(
      fn -> preferred_fetch(client, partition, rack_id, expected) end,
      "preferred_read_replica #{expected} for partition #{partition}, rack #{inspect(rack_id)}"
    )
  end

  defp preferred_fetch(client, partition, rack_id, expected) do
    case API.fetch(client, @topic, partition, 0, rack_id: rack_id, min_bytes: 0) do
      {:ok, %Fetch{preferred_read_replica: ^expected} = result} -> result
      _ -> nil
    end
  end

  defp start_client! do
    {:ok, args} = KafkaEx.build_worker_options([])
    {:ok, pid} = Client.start_link(args, :no_name)
    pid
  end

  defp with_fresh_client(fun) do
    pid = start_client!()

    try do
      fun.(pid)
    after
      stop_safely(pid)
    end
  end

  # Metadata for one missing name auto-creates the topic. An empty list does not.
  defp cluster_metadata!(client) do
    {:ok, cm} = API.metadata(client, nil, @metadata_opts)
    cm
  end

  defp topic(client), do: Map.get(cluster_metadata!(client).topics, @topic)
  defp topic!(cm), do: Map.fetch!(cm.topics, @topic)

  defp partition!(cm, partition_id) do
    Enum.find(topic!(cm).partitions, &(&1.partition_id == partition_id)) ||
      flunk("partition #{partition_id} missing from the test topic")
  end

  defp partition_ids(cm), do: Enum.map(topic!(cm).partitions, & &1.partition_id)

  defp client_racks(cm) do
    cm.brokers |> Map.values() |> Enum.map(& &1.rack) |> Enum.reject(&(&1 in [nil, ""])) |> Enum.uniq()
  end

  defp rack!(cm, node_id) do
    case Map.fetch(cm.brokers, node_id) do
      {:ok, %{rack: rack}} when is_binary(rack) and rack != "" -> rack
      _ -> flunk("broker #{node_id} has no rack in metadata")
    end
  end

  # -1 when the leader is already in the client's rack, or that rack has no replica.
  defp preferred_read_replica(cm, partition_id, client_rack) do
    %PartitionInfo{leader: leader, replicas: replicas} = partition!(cm, partition_id)

    if rack!(cm, leader) == client_rack do
      -1
    else
      Enum.find(replicas, fn node_id -> rack!(cm, node_id) == client_rack end) || -1
    end
  end

  defp rack_cases(cm, wanted?) do
    for partition <- partition_ids(cm),
        client_rack <- client_racks(cm),
        preferred = preferred_read_replica(cm, partition, client_rack),
        wanted?.(preferred),
        do: {partition, client_rack, preferred}
  end

  # A delete stays in flight after metadata drops the name, and can remove a
  # topic created in that window. Create while the topic is absent. Delete
  # only a topic with the wrong replica assignment.
  defp ensure_replicated_topic!(client) do
    wait_until(
      fn -> topic_ready(client) end,
      "the test topic to have #{@partition_count} partitions with #{@replication_factor} in-sync replicas"
    )
  end

  defp topic_ready(client) do
    case topic(client) do
      nil ->
        create_replicated_topic!(client)
        nil

      topic ->
        keep_or_replace(client, topic)
    end
  end

  defp keep_or_replace(client, %{partitions: partitions} = topic) do
    cond do
      length(partitions) == @partition_count and Enum.all?(partitions, &replicated_partition?/1) ->
        topic

      length(partitions) != @partition_count or Enum.any?(partitions, &wrong_replica_count?/1) ->
        _ = API.delete_topic(client, @topic)
        nil

      true ->
        nil
    end
  end

  defp replicated_partition?(%PartitionInfo{leader: leader, replicas: replicas, isr: isr}) do
    leader >= 0 and length(replicas) == @replication_factor and length(isr) == @replication_factor
  end

  defp wrong_replica_count?(%PartitionInfo{replicas: replicas}), do: length(replicas) != @replication_factor

  defp create_replicated_topic!(client) do
    {:ok, %CreateTopics{} = result} =
      API.create_topic(client, @topic,
        num_partitions: @partition_count,
        replication_factor: @replication_factor
      )

    case CreateTopics.get_topic_result(result, @topic) do
      %{error: error} when error in [:no_error, :topic_already_exists] -> :ok
      other -> flunk("failed to create the test topic: #{inspect(other)}")
    end
  end

  # `nil` and `false` mean "not yet". Anything else is the value the caller waited for.
  defp wait_until(fun, description, deadline \\ nil)

  defp wait_until(fun, description, nil) do
    wait_until(fun, description, System.monotonic_time(:millisecond) + @topic_ready_timeout_ms)
  end

  defp wait_until(fun, description, deadline) do
    case fun.() do
      result when result in [nil, false] -> continue_wait(fun, description, deadline)
      result -> result
    end
  end

  defp continue_wait(fun, description, deadline) do
    if System.monotonic_time(:millisecond) > deadline do
      flunk("timed out after #{@topic_ready_timeout_ms}ms waiting for #{description}")
    else
      Process.sleep(200)
      wait_until(fun, description, deadline)
    end
  end
end
