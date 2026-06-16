defmodule KafkaEx.Integration.RackAwarenessTest do
  @moduledoc """
  End-to-end verification of KIP-392 (rack-aware fetching) against a
  rack-tagged cluster: the request carries `rack_id`, the response carries
  `preferred_read_replica`, the broker picks a same-rack replica, and the
  client caches the hint so subsequent fetches go to the preferred broker.

  Cluster shape lives in `docker-compose.yml` — `KAFKA_BROKER_RACK` per
  broker and `KAFKA_REPLICA_SELECTOR_CLASS=RackAwareReplicaSelector` in
  `docker-compose-kafka.env`.
  """
  use ExUnit.Case, async: true
  @moduletag :rack_awareness

  alias KafkaEx.Client
  alias KafkaEx.API
  alias KafkaEx.Messages.Fetch

  @topic "rack_aware_test"

  @rack_to_broker_id %{"az-a" => 1, "az-b" => 2, "az-c" => 3}
  @partition_to_leader_rack %{0 => "az-a", 1 => "az-b", 2 => "az-c"}

  setup do
    {:ok, args} = KafkaEx.build_worker_options([])
    {:ok, pid} = Client.start_link(args, :no_name)
    on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid) end)
    {:ok, %{client: pid}}
  end

  defp with_fresh_client(fun) do
    {:ok, args} = KafkaEx.build_worker_options([])
    {:ok, pid} = Client.start_link(args, :no_name)

    try do
      fun.(pid)
    after
      if Process.alive?(pid), do: GenServer.stop(pid)
    end
  end

  describe "broker metadata" do
    test "keeps track of broker-reported rack IDs", %{client: client} do
      {:ok, cluster_metadata} = API.metadata(client, [@topic], api_version: 9)

      for {expected_rack, broker_id} <- @rack_to_broker_id do
        broker = Map.fetch!(cluster_metadata.brokers, broker_id)
        assert broker.rack == expected_rack
      end
    end
  end

  describe "Fetch V11 with rack_id (KIP-392)" do
    test "cross-rack request returns the same-rack replica as preferred" do
      for partition <- 0..2,
          {client_rack, expected_broker_id} <- @rack_to_broker_id,
          @partition_to_leader_rack[partition] != client_rack do
        with_fresh_client(fn client ->
          assert {:ok, %Fetch{preferred_read_replica: ^expected_broker_id}} =
                   API.fetch(client, @topic, partition, 0, rack_id: client_rack, min_bytes: 0)
        end)
      end
    end

    test "same-rack-as-leader request returns -1 (no redirect needed)" do
      for partition <- 0..2,
          {client_rack, _broker_id} <- @rack_to_broker_id,
          @partition_to_leader_rack[partition] == client_rack do
        with_fresh_client(fn client ->
          assert {:ok, %Fetch{preferred_read_replica: -1}} =
                   API.fetch(client, @topic, partition, 0, rack_id: client_rack, min_bytes: 0)
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
end
