defmodule KafkaEx.Messages.ConsumerGroupDescriptionTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Messages.ConsumerGroupDescription

  describe "from_describe_group_response/1" do
    test "returns a consumer group description struct" do
      response = %{
        group_id: "test-group",
        state: "Stable",
        protocol_type: "consumer",
        protocol: "range",
        members: [
          %{
            member_id: "test-member",
            client_id: "test-client",
            client_host: "test-host",
            member_metadata: "test-metadata",
            member_assignment: %{
              version: 0,
              user_data: "test-user-data",
              partition_assignments: [
                %{topic: "test-topic", partitions: [1, 2, 3]}
              ]
            }
          }
        ]
      }

      expected = %ConsumerGroupDescription{
        group_id: "test-group",
        state: "Stable",
        protocol_type: "consumer",
        protocol: "range",
        members: [
          %ConsumerGroupDescription.Member{
            member_id: "test-member",
            client_id: "test-client",
            client_host: "test-host",
            member_metadata: "test-metadata",
            member_assignment: %ConsumerGroupDescription.Member.MemberAssignment{
              version: 0,
              user_data: "test-user-data",
              partition_assignments: [
                %ConsumerGroupDescription.Member.MemberAssignment.PartitionAssignment{
                  topic: "test-topic",
                  partitions: [1, 2, 3]
                }
              ]
            }
          }
        ]
      }

      assert ConsumerGroupDescription.from_describe_group_response(response) == expected
    end
  end

  describe "accessor functions" do
    setup do
      description = %ConsumerGroupDescription{
        group_id: "my-group",
        state: "Stable",
        protocol_type: "consumer",
        protocol: "range",
        members: []
      }

      {:ok, %{description: description}}
    end

    test "group_id/1 returns the group id", %{description: description} do
      assert ConsumerGroupDescription.group_id(description) == "my-group"
    end

    test "state/1 returns the state", %{description: description} do
      assert ConsumerGroupDescription.state(description) == "Stable"
    end

    test "members/1 returns the members list", %{description: description} do
      assert ConsumerGroupDescription.members(description) == []
    end

    test "stable?/1 returns true when state is Stable", %{description: description} do
      assert ConsumerGroupDescription.stable?(description) == true
    end

    test "stable?/1 returns false when state is not Stable" do
      description = %ConsumerGroupDescription{state: "PreparingRebalance"}
      assert ConsumerGroupDescription.stable?(description) == false
    end
  end
end
