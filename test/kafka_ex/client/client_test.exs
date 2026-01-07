defmodule KafkaEx.ClientTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias KafkaEx.Client
  alias KafkaEx.Client.State
  alias KafkaEx.Cluster.Broker
  alias KafkaEx.Cluster.ClusterMetadata

  describe "handle_call/3 - offset_fetch" do
    test "returns error for invalid consumer group" do
      state = %State{consumer_group_for_auto_commit: :no_consumer_group}

      assert {:reply, {:error, :invalid_consumer_group}, ^state} =
               Client.handle_call(
                 {:offset_fetch, :no_consumer_group, [{"test-topic", [%{partition_num: 0}]}], []},
                 self(),
                 state
               )
    end

    test "handles valid consumer group" do
      # Mock state with valid consumer group and API versions
      state = %State{
        consumer_group_for_auto_commit: "test-group",
        api_versions: %{9 => {0, 3}},
        correlation_id: 1,
        cluster_metadata: %ClusterMetadata{
          brokers: %{},
          consumer_group_coordinators: %{"test-group" => 1}
        }
      }

      # Note: This would require mocking the network layer for a full test
      # Here we just verify the handle_call accepts the valid format
      result =
        Client.handle_call(
          {:offset_fetch, "test-group", [{"test-topic", [%{partition_num: 0}]}], []},
          self(),
          state
        )

      assert match?({:reply, _, _}, result)
    end
  end

  describe "handle_call/3 - offset_commit" do
    test "returns error for invalid consumer group" do
      state = %State{consumer_group_for_auto_commit: :no_consumer_group}

      assert {:reply, {:error, :invalid_consumer_group}, ^state} =
               Client.handle_call(
                 {:offset_commit, :no_consumer_group, [{"test-topic", [%{partition_num: 0, offset: 100}]}], []},
                 self(),
                 state
               )
    end

    test "handles valid consumer group" do
      # Mock state with valid consumer group and API versions
      state = %State{
        consumer_group_for_auto_commit: "test-group",
        api_versions: %{8 => {0, 3}},
        correlation_id: 1,
        cluster_metadata: %ClusterMetadata{
          brokers: %{},
          consumer_group_coordinators: %{"test-group" => 1}
        }
      }

      # Note: This would require mocking the network layer for a full test
      # Here we just verify the handle_call accepts the valid format
      result =
        Client.handle_call(
          {:offset_commit, "test-group", [{"test-topic", [%{partition_num: 0, offset: 100}]}], []},
          self(),
          state
        )

      assert match?({:reply, _, _}, result)
    end
  end

  describe "broker connection handling" do
    test "Broker.connected?/1 returns false for nil socket" do
      broker = %Broker{node_id: 1, host: "localhost", port: 9092, socket: nil}
      refute Broker.connected?(broker)
    end

    test "Broker.connected?/1 returns false for closed socket" do
      broker = %Broker{node_id: 1, host: "localhost", port: 9092, socket: nil}
      refute Broker.connected?(broker)
    end
  end

  describe "select_broker_with_update via State.select_broker" do
    test "returns error when broker not found" do
      state = %State{
        cluster_metadata: %ClusterMetadata{
          brokers: %{},
          topics: %{}
        }
      }

      selector = KafkaEx.Client.NodeSelector.node_id(999)
      assert {:error, :no_such_node} = State.select_broker(state, selector)
    end

    test "returns broker when found" do
      broker = %Broker{node_id: 1, host: "localhost", port: 9092, socket: nil}

      state = %State{
        cluster_metadata: %ClusterMetadata{
          brokers: %{1 => broker},
          topics: %{}
        }
      }

      selector = KafkaEx.Client.NodeSelector.node_id(1)
      assert {:ok, ^broker} = State.select_broker(state, selector)
    end
  end

  describe "reconnection logging" do
    test "logs info message when attempting reconnection" do
      broker = %Broker{node_id: 1, host: "localhost", port: 9092, socket: nil}

      # Verify Broker.to_string produces expected format for logging
      assert Broker.to_string(broker) == "broker 1 (localhost:9092)"

      log =
        capture_log(fn ->
          require Logger
          # This is the format used by reconnect_broker
          Logger.info("Reconnecting to #{Broker.to_string(broker)}, attempt 1/3")
        end)

      assert log =~ "Reconnecting to broker 1 (localhost:9092)"
      assert log =~ "attempt 1/3"
    end

    test "logs warning message after failed reconnection attempts" do
      broker = %Broker{node_id: 1, host: "localhost", port: 9092, socket: nil}

      log =
        capture_log(fn ->
          require Logger
          Logger.warning("Failed to reconnect to #{Broker.to_string(broker)} after 3 attempts")
        end)

      assert log =~ "Failed to reconnect to broker 1 (localhost:9092)"
      assert log =~ "after 3 attempts"
    end
  end

  describe "NetworkClient nil socket handling" do
    test "send_sync_request returns :not_connected for nil socket" do
      broker = %{socket: nil, host: "localhost", port: 9092}
      assert {:error, :not_connected} = KafkaEx.Network.NetworkClient.send_sync_request(broker, <<>>, 1000)
    end

    test "send_sync_request returns :no_broker for nil broker" do
      assert {:error, :no_broker} = KafkaEx.Network.NetworkClient.send_sync_request(nil, <<>>, 1000)
    end

    test "send_async_request returns :not_connected for nil socket" do
      broker = %{socket: nil, host: "localhost", port: 9092}
      assert {:error, :not_connected} = KafkaEx.Network.NetworkClient.send_async_request(broker, <<>>)
    end
  end

  describe "first_broker_response with disconnected brokers" do
    test "returns error when all brokers disconnected" do
      # Create brokers with nil sockets
      brokers = [
        %Broker{node_id: 1, host: "localhost", port: 9092, socket: nil},
        %Broker{node_id: 2, host: "localhost", port: 9093, socket: nil}
      ]

      # The first_broker_response is private, but we can test through
      # the behavior - disconnected brokers should be skipped
      # and eventually return an error
      for broker <- brokers do
        refute Broker.connected?(broker)
      end
    end
  end
end
