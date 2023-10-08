defmodule KafkaEx.New.Protocols.Kayrock.DescribeGroupsTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Protocols.Kayrock.DescribeGroups, as: KayrockDescribeGroups

  alias Kayrock.DescribeGroups.V0

  describe "build_request/2" do
    test "builds request for Describe Groups API" do
      api_version = 0
      consumer_group_names = ["test-group"]
      expected_request = %V0.Request{group_ids: groups}

      assert KayrockDescribeGroups.build_request(
               api_version,
               consumer_group_names
             ) == expected_request
    end
  end

  describe "build_response/1" do
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

      assert {:ok,
              [
                %KafkaEx.New.ConsumerGroup{
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
                      member_assignment:
                        %KafkaEx.New.ConsumerGroup.Member.MemberAssignment{
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
              ]} ==
               DescribeGroups.Response.parse_response(response)
    end
  end
end
