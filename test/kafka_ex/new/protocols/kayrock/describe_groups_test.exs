defmodule KafkaEx.New.Protocols.DescribeGroupsTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Protocols.DescribeGroups

  alias Kayrock.DescribeGroups.V0
  alias Kayrock.DescribeGroups.V1

  describe "build_request/2" do
    test "api version 0 - build describe group request" do
      groups = ["group1", "group2"]

      assert %V0.Request{
               group_ids: groups
             } == DescribeGroups.Request.build_request(%V0.Request{}, groups, 0)
    end

    test "api version 1 - build describe group request" do
      groups = ["group1", "group2"]

      assert %V1.Request{
               group_ids: groups
             } == DescribeGroups.Request.build_request(%V1.Request{}, groups, 1)
    end
  end

  describe "parse_response/1" do
    @expected_group %KafkaEx.New.ConsumerGroup{
      group_id: "succeeded",
      state: "stable",
      protocol_type: "protocol_type",
      protocol: "protocol",
      members: [
        %KafkaEx.New.ConsumerGroup.Member{
          member_id: "member_id",
          client_id: "client_id",
          client_host: "client_host",
          member_metadata: "member_metadata",
          member_assignment: %KafkaEx.New.ConsumerGroup.Member.MemberAssignment{
            version: 0,
            user_data: "user_data",
            partition_assignments: [
              %KafkaEx.New.ConsumerGroup.Member.MemberAssignment.PartitionAssignment{
                topic: "test-topic",
                partitions: [1, 2, 3]
              }
            ]
          }
        }
      ]
    }

    test "api version 0 - returns response if all groups succeeded" do
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
               DescribeGroups.Response.parse_response(response)
    end

    test "api version 0 - returns error if any group failed" do
      response = %V0.Response{
        groups: [
          %{group_id: "succeeded", error_code: 0},
          %{group_id: "failed", error_code: 1}
        ]
      }

      assert {:error, [{"failed", :offset_out_of_range}]} ==
               DescribeGroups.Response.parse_response(response)
    end

    test "api version 1 - returns response if all groups succeeded" do
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
               DescribeGroups.Response.parse_response(response)
    end

    test "api version 1 - returns error if any group failed" do
      response = %V1.Response{
        groups: [
          %{group_id: "succeeded", error_code: 0},
          %{group_id: "failed", error_code: 1}
        ]
      }

      assert {:error, [{"failed", :offset_out_of_range}]} ==
               DescribeGroups.Response.parse_response(response)
    end
  end
end
