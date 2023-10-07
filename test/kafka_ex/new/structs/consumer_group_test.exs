defmodule KafkaEx.New.ConsumerGroupTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.ConsumerGroup

  describe "from_describe_group_response/1" do
    test "returns a consumer group struct" do
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

      expected = %ConsumerGroup{
        group_id: "test-group",
        state: "Stable",
        protocol_type: "consumer",
        protocol: "range",
        members: [
          %ConsumerGroup.Member{
            member_id: "test-member",
            client_id: "test-client",
            client_host: "test-host",
            member_metadata: "test-metadata",
            member_assignment: %ConsumerGroup.Member.MemberAssignment{
              version: 0,
              user_data: "test-user-data",
              partition_assignments: [
                %ConsumerGroup.Member.MemberAssignment.PartitionAssignment{
                  topic: "test-topic",
                  partitions: [1, 2, 3]
                }
              ]
            }
          }
        ]
      }

      assert ConsumerGroup.from_describe_group_response(response) == expected
    end
  end
end
