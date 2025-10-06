defmodule KafkaEx.New.Protocols.Kayrock.SyncGroup.V1ResponseImplTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Protocols.Kayrock.SyncGroup
  alias KafkaEx.New.Structs.ConsumerGroup.Member.MemberAssignment.PartitionAssignment

  describe "parse_response/1 for V1" do
    test "parses successful response with no error and empty assignments" do
      member_assignment = %Kayrock.MemberAssignment{
        version: 0,
        partition_assignments: [],
        user_data: ""
      }

      response = %Kayrock.SyncGroup.V1.Response{
        throttle_time_ms: 100,
        error_code: 0,
        member_assignment: member_assignment
      }

      assert {:ok, result} = SyncGroup.Response.parse_response(response)
      assert result.throttle_time_ms == 100
      assert result.partition_assignments == []
    end

    test "parses successful response with zero throttle time" do
      member_assignment = %Kayrock.MemberAssignment{
        version: 0,
        partition_assignments: [],
        user_data: ""
      }

      response = %Kayrock.SyncGroup.V1.Response{
        throttle_time_ms: 0,
        error_code: 0,
        member_assignment: member_assignment
      }

      assert {:ok, result} = SyncGroup.Response.parse_response(response)
      assert result.throttle_time_ms == 0
    end

    test "parses successful response with partition assignments" do
      member_assignment = %Kayrock.MemberAssignment{
        version: 0,
        partition_assignments: [
          %Kayrock.MemberAssignment.PartitionAssignment{
            topic: "test-topic",
            partitions: [0, 1, 2]
          }
        ],
        user_data: ""
      }

      response = %Kayrock.SyncGroup.V1.Response{
        throttle_time_ms: 50,
        error_code: 0,
        member_assignment: member_assignment
      }

      assert {:ok, result} = SyncGroup.Response.parse_response(response)
      assert result.throttle_time_ms == 50
      assert length(result.partition_assignments) == 1

      [assignment] = result.partition_assignments
      assert %PartitionAssignment{topic: "test-topic", partitions: [0, 1, 2]} = assignment
    end

    test "parses successful response with multiple topics" do
      member_assignment = %Kayrock.MemberAssignment{
        version: 0,
        partition_assignments: [
          %Kayrock.MemberAssignment.PartitionAssignment{
            topic: "topic-1",
            partitions: [0, 1]
          },
          %Kayrock.MemberAssignment.PartitionAssignment{
            topic: "topic-2",
            partitions: [3, 4, 5]
          },
          %Kayrock.MemberAssignment.PartitionAssignment{
            topic: "topic-3",
            partitions: [10]
          }
        ],
        user_data: ""
      }

      response = %Kayrock.SyncGroup.V1.Response{
        throttle_time_ms: 25,
        error_code: 0,
        member_assignment: member_assignment
      }

      assert {:ok, result} = SyncGroup.Response.parse_response(response)
      assert result.throttle_time_ms == 25
      assert length(result.partition_assignments) == 3
    end

    test "parses error response with unknown_member_id" do
      member_assignment = %Kayrock.MemberAssignment{
        version: 0,
        partition_assignments: [],
        user_data: ""
      }

      response = %Kayrock.SyncGroup.V1.Response{
        throttle_time_ms: 10,
        error_code: 25,
        member_assignment: member_assignment
      }

      assert {:error, error} = SyncGroup.Response.parse_response(response)
      assert error.error == :unknown_member_id
      assert error.metadata == %{}
    end

    test "parses error response with rebalance_in_progress" do
      member_assignment = %Kayrock.MemberAssignment{
        version: 0,
        partition_assignments: [],
        user_data: ""
      }

      response = %Kayrock.SyncGroup.V1.Response{
        throttle_time_ms: 0,
        error_code: 27,
        member_assignment: member_assignment
      }

      assert {:error, error} = SyncGroup.Response.parse_response(response)
      assert error.error == :rebalance_in_progress
    end

    test "parses error response with illegal_generation" do
      member_assignment = %Kayrock.MemberAssignment{
        version: 0,
        partition_assignments: [],
        user_data: ""
      }

      response = %Kayrock.SyncGroup.V1.Response{
        throttle_time_ms: 5,
        error_code: 22,
        member_assignment: member_assignment
      }

      assert {:error, error} = SyncGroup.Response.parse_response(response)
      assert error.error == :illegal_generation
    end

    test "parses error response with coordinator_not_available" do
      member_assignment = %Kayrock.MemberAssignment{
        version: 0,
        partition_assignments: [],
        user_data: ""
      }

      response = %Kayrock.SyncGroup.V1.Response{
        throttle_time_ms: 15,
        error_code: 15,
        member_assignment: member_assignment
      }

      assert {:error, error} = SyncGroup.Response.parse_response(response)
      assert error.error == :coordinator_not_available
    end

    test "parses error response with not_coordinator" do
      member_assignment = %Kayrock.MemberAssignment{
        version: 0,
        partition_assignments: [],
        user_data: ""
      }

      response = %Kayrock.SyncGroup.V1.Response{
        throttle_time_ms: 20,
        error_code: 16,
        member_assignment: member_assignment
      }

      assert {:error, error} = SyncGroup.Response.parse_response(response)
      assert error.error == :not_coordinator
    end

    test "parses error response with group_authorization_failed" do
      member_assignment = %Kayrock.MemberAssignment{
        version: 0,
        partition_assignments: [],
        user_data: ""
      }

      response = %Kayrock.SyncGroup.V1.Response{
        throttle_time_ms: 30,
        error_code: 30,
        member_assignment: member_assignment
      }

      assert {:error, error} = SyncGroup.Response.parse_response(response)
      assert error.error == :group_authorization_failed
    end

    test "error struct has empty metadata" do
      member_assignment = %Kayrock.MemberAssignment{
        version: 0,
        partition_assignments: [],
        user_data: ""
      }

      response = %Kayrock.SyncGroup.V1.Response{
        throttle_time_ms: 40,
        error_code: 27,
        member_assignment: member_assignment
      }

      {:error, error} = SyncGroup.Response.parse_response(response)

      assert error.metadata == %{}
    end
  end
end
