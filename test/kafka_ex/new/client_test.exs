defmodule KafkaEx.New.ClientTest do
  use ExUnit.Case, async: false

  alias KafkaEx.New.Client
  alias KafkaEx.New.Client.State

  @moduletag :new_client

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
        cluster_metadata: %KafkaEx.New.Kafka.ClusterMetadata{
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
        cluster_metadata: %KafkaEx.New.Kafka.ClusterMetadata{
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
end
