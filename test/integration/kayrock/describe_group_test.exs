defmodule KafkaEx.DescribeGroups.Test do
  @moduledoc """
  Tests for describing consumer groups using new API
  """
  use ExUnit.Case, async: false

  @moduletag :new_client

  setup do
    client_name = TestHelper.generate_random_string()
    consumer_group = TestHelper.generate_random_string()

    {:ok, client_pid} =
      KafkaEx.start_link_worker(:no_name,
        consumer_group: client_name,
        server_impl: KafkaEx.New.Client
      )

    {:ok, %{client: client_pid, consumer_group: consumer_group}}
  end

  describe "describe_group/2" do
    test "returns describe data response when there is active consumer group ",
         %{client: client_pid, consumer_group: consumer_group} do
      # [WHEN] there is topic in kafka
      topic_name = TestHelper.generate_random_string()
      partitions_num = 2

      KafkaEx.create_topics(
        [
          %{
            topic: topic_name,
            num_partitions: partitions_num,
            replication_factor: 1,
            replica_assignment: [],
            config_entries: [
              %{config_name: "cleanup.policy", config_value: "compact"},
              %{config_name: "min.compaction.lag.ms", config_value: "0"}
            ]
          }
        ],
        timeout: 10_000
      )

      # [WHEN] There is active consumer group
      consumer_group_opts = [
        heartbeat_interval: 500,
        commit_interval: 500
      ]

      {:ok, _pid} =
        KafkaEx.ConsumerGroup.start_link(
          ExampleGenConsumer,
          consumer_group,
          [topic_name],
          consumer_group_opts
        )

      :timer.sleep(1000)

      # [THEN] Receives consumer group description
      {:ok, response} =
        KafkaEx.New.KafkaExAPI.describe_group(client_pid, consumer_group)

      assert !is_nil(response.correlation_id)

      [group_description] = response.groups
      assert 0 == group_description.error_code
      assert consumer_group == group_description.group_id
      assert "Stable" == group_description.state

      [member] = group_description.members
      assert "kafka_ex" == member.client_id
      assert !is_nil(member.member_id)

      [partition_assignments] = member.member_assignment.partition_assignments
      assert partitions_num == length(partition_assignments.partitions)
      assert topic_name == partition_assignments.topic
    end

    test "returns describe data response when there is no active consumer group ",
         %{client: client} do
      # [THEN] Receives consumer group description
      {:ok, response} =
        KafkaEx.New.KafkaExAPI.describe_group(client, "missing-group-name")

      # Created consumer group name
      [group_description] = response.groups

      assert 0 == group_description.error_code
      assert "missing-group-name" == group_description.group_id
      assert [] == group_description.members
      assert "Dead" == group_description.state
    end
  end
end
