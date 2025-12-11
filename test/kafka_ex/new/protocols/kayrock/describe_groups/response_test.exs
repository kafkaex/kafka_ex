defmodule KafkaEx.New.Protocols.Kayrock.DescribeGroups.ResponseTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Protocols.Kayrock.DescribeGroups.Response,
    as: DescribeGroupsResponse

  alias Kayrock.DescribeGroups.V0
  alias Kayrock.DescribeGroups.V1

  describe "parse_response/1" do
    @expected_group %KafkaEx.New.Kafka.ConsumerGroupDescription{
      group_id: "succeeded",
      state: "stable",
      protocol_type: "protocol_type",
      protocol: "protocol",
      members: [
        %KafkaEx.New.Kafka.ConsumerGroupDescription.Member{
          member_id: "member_id",
          client_id: "client_id",
          client_host: "client_host",
          member_metadata: "member_metadata",
          member_assignment: %KafkaEx.New.Kafka.ConsumerGroupDescription.Member.MemberAssignment{
            version: 0,
            user_data: "user_data",
            partition_assignments: [
              %KafkaEx.New.Kafka.ConsumerGroupDescription.Member.MemberAssignment.PartitionAssignment{
                topic: "test-topic",
                partitions: [1, 2, 3]
              }
            ]
          }
        }
      ]
    }

    test "for api version 0 - returns response if all groups succeeded" do
      response = %V0.Response{
        groups: [
          %{
            group_id: "succeeded",
            error_code: 0,
            state: "stable",
            protocol_type: "protocol_type",
            protocol: "protocol",
            members: [
              %{
                member_id: "member_id",
                client_id: "client_id",
                client_host: "client_host",
                member_metadata: "member_metadata",
                member_assignment: %{
                  version: 0,
                  user_data: "user_data",
                  partition_assignments: [
                    %{topic: "test-topic", partitions: [1, 2, 3]}
                  ]
                }
              }
            ]
          }
        ]
      }

      assert {:ok, [@expected_group]} ==
               DescribeGroupsResponse.parse_response(response)
    end

    test "for api version 0 - returns error if any group failed" do
      response = %V0.Response{
        groups: [
          %{group_id: "succeeded", error_code: 0},
          %{group_id: "failed", error_code: 1}
        ]
      }

      assert {:error, [{"failed", :offset_out_of_range}]} ==
               DescribeGroupsResponse.parse_response(response)
    end

    test "for api version 1 - returns response if all groups succeeded" do
      response = %V1.Response{
        groups: [
          %{
            group_id: "succeeded",
            error_code: 0,
            state: "stable",
            protocol_type: "protocol_type",
            protocol: "protocol",
            members: [
              %{
                member_id: "member_id",
                client_id: "client_id",
                client_host: "client_host",
                member_metadata: "member_metadata",
                member_assignment: %{
                  version: 0,
                  user_data: "user_data",
                  partition_assignments: [
                    %{topic: "test-topic", partitions: [1, 2, 3]}
                  ]
                }
              }
            ]
          }
        ]
      }

      assert {:ok, [@expected_group]} ==
               DescribeGroupsResponse.parse_response(response)
    end

    test "for api version 1 - returns error if any group failed" do
      response = %V1.Response{
        groups: [
          %{group_id: "succeeded", error_code: 0},
          %{group_id: "failed", error_code: 1}
        ]
      }

      assert {:error, [{"failed", :offset_out_of_range}]} ==
               DescribeGroupsResponse.parse_response(response)
    end
  end
end
