defmodule KafkaEx.New.Adapter.SyncGroupTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Adapter
  alias KafkaEx.Protocol.SyncGroup.Request, as: SyncGroupRequest
  alias KafkaEx.Protocol.SyncGroup.Response, as: SyncGroupResponse
  alias Kayrock.SyncGroup

  describe "sync_group_request/1 - legacy to new API" do
    test "converts legacy SyncGroupRequest to Kayrock V0 request" do
      legacy_request = %SyncGroupRequest{
        group_name: "test-group",
        member_id: "consumer-123",
        generation_id: 5,
        assignments: []
      }

      {kayrock_request, consumer_group} = Adapter.sync_group_request(legacy_request)

      assert consumer_group == "test-group"

      assert kayrock_request == %SyncGroup.V0.Request{
               group_id: "test-group",
               member_id: "consumer-123",
               generation_id: 5,
               group_assignment: []
             }
    end

    test "handles empty member_id" do
      legacy_request = %SyncGroupRequest{
        group_name: "my-group",
        member_id: "",
        generation_id: 0,
        assignments: []
      }

      {kayrock_request, consumer_group} = Adapter.sync_group_request(legacy_request)

      assert consumer_group == "my-group"
      assert kayrock_request.member_id == ""
      assert kayrock_request.generation_id == 0
    end

    test "handles generation_id 0" do
      legacy_request = %SyncGroupRequest{
        group_name: "group",
        member_id: "member",
        generation_id: 0,
        assignments: []
      }

      {kayrock_request, _consumer_group} = Adapter.sync_group_request(legacy_request)

      assert kayrock_request.generation_id == 0
    end

    test "handles large generation_id" do
      legacy_request = %SyncGroupRequest{
        group_name: "group",
        member_id: "member",
        generation_id: 999_999,
        assignments: []
      }

      {kayrock_request, _consumer_group} = Adapter.sync_group_request(legacy_request)

      assert kayrock_request.generation_id == 999_999
    end

    test "handles empty assignments list" do
      legacy_request = %SyncGroupRequest{
        group_name: "group",
        member_id: "member",
        generation_id: 1,
        assignments: []
      }

      {kayrock_request, _consumer_group} = Adapter.sync_group_request(legacy_request)

      assert kayrock_request.group_assignment == []
    end

    test "converts assignments with partition data" do
      legacy_request = %SyncGroupRequest{
        group_name: "group",
        member_id: "leader-member",
        generation_id: 1,
        assignments: [
          {"member-1", [{"topic-1", [0, 1, 2]}]},
          {"member-2", [{"topic-2", [3, 4]}]}
        ]
      }

      {kayrock_request, _consumer_group} = Adapter.sync_group_request(legacy_request)

      assert length(kayrock_request.group_assignment) == 2

      # Verify structure
      [assignment1, assignment2] = kayrock_request.group_assignment
      assert assignment1.member_id == "member-1"
      assert assignment2.member_id == "member-2"
    end

    test "handles unicode characters in group_name and member_id" do
      legacy_request = %SyncGroupRequest{
        group_name: "group-日本",
        member_id: "member-한국",
        generation_id: 1,
        assignments: []
      }

      {kayrock_request, consumer_group} = Adapter.sync_group_request(legacy_request)

      assert consumer_group == "group-日本"
      assert kayrock_request.group_id == "group-日本"
      assert kayrock_request.member_id == "member-한국"
    end

    test "handles long string values" do
      long_group = String.duplicate("a", 500)
      long_member = String.duplicate("b", 500)

      legacy_request = %SyncGroupRequest{
        group_name: long_group,
        member_id: long_member,
        generation_id: 1,
        assignments: []
      }

      {kayrock_request, consumer_group} = Adapter.sync_group_request(legacy_request)

      assert consumer_group == long_group
      assert kayrock_request.group_id == long_group
      assert kayrock_request.member_id == long_member
    end
  end

  describe "sync_group_response/1 - new to legacy API (V0)" do
    test "converts successful V0 response to legacy format" do
      kayrock_response = %SyncGroup.V0.Response{
        error_code: 0,
        member_assignment: %Kayrock.MemberAssignment{
          partition_assignments: []
        }
      }

      legacy_response = Adapter.sync_group_response(kayrock_response)

      assert %SyncGroupResponse{
               error_code: :no_error,
               assignments: []
             } = legacy_response
    end

    test "converts V0 response with partition assignments" do
      kayrock_response = %SyncGroup.V0.Response{
        error_code: 0,
        member_assignment: %Kayrock.MemberAssignment{
          partition_assignments: [
            %{topic: "topic-1", partitions: [0, 1, 2]},
            %{topic: "topic-2", partitions: [3]}
          ]
        }
      }

      legacy_response = Adapter.sync_group_response(kayrock_response)

      assert %SyncGroupResponse{error_code: :no_error} = legacy_response
      assert length(legacy_response.assignments) == 2
      assert {"topic-1", [0, 1, 2]} in legacy_response.assignments
      assert {"topic-2", [3]} in legacy_response.assignments
    end

    test "converts V0 error response (unknown_member_id)" do
      kayrock_response = %SyncGroup.V0.Response{
        error_code: 25,
        member_assignment: %Kayrock.MemberAssignment{partition_assignments: []}
      }

      legacy_response = Adapter.sync_group_response(kayrock_response)

      assert %SyncGroupResponse{error_code: :unknown_member_id} = legacy_response
    end

    test "converts V0 error response (illegal_generation)" do
      kayrock_response = %SyncGroup.V0.Response{
        error_code: 22,
        member_assignment: %Kayrock.MemberAssignment{partition_assignments: []}
      }

      legacy_response = Adapter.sync_group_response(kayrock_response)

      assert %SyncGroupResponse{error_code: :illegal_generation} = legacy_response
    end

    test "converts V0 error response (rebalance_in_progress)" do
      kayrock_response = %SyncGroup.V0.Response{
        error_code: 27,
        member_assignment: %Kayrock.MemberAssignment{partition_assignments: []}
      }

      legacy_response = Adapter.sync_group_response(kayrock_response)

      assert %SyncGroupResponse{error_code: :rebalance_in_progress} = legacy_response
    end

    test "converts V0 error response (not_coordinator)" do
      kayrock_response = %SyncGroup.V0.Response{
        error_code: 16,
        member_assignment: %Kayrock.MemberAssignment{partition_assignments: []}
      }

      legacy_response = Adapter.sync_group_response(kayrock_response)

      assert %SyncGroupResponse{error_code: :not_coordinator} = legacy_response
    end

    test "converts V0 error response (coordinator_not_available)" do
      kayrock_response = %SyncGroup.V0.Response{
        error_code: 15,
        member_assignment: %Kayrock.MemberAssignment{partition_assignments: []}
      }

      legacy_response = Adapter.sync_group_response(kayrock_response)

      assert %SyncGroupResponse{error_code: :coordinator_not_available} = legacy_response
    end

    test "converts V0 error response (group_authorization_failed)" do
      kayrock_response = %SyncGroup.V0.Response{
        error_code: 30,
        member_assignment: %Kayrock.MemberAssignment{partition_assignments: []}
      }

      legacy_response = Adapter.sync_group_response(kayrock_response)

      assert %SyncGroupResponse{error_code: :group_authorization_failed} = legacy_response
    end
  end

  describe "sync_group_response/1 - new to legacy API (V1)" do
    test "converts successful V1 response to legacy format (ignores throttle_time_ms)" do
      kayrock_response = %SyncGroup.V1.Response{
        error_code: 0,
        throttle_time_ms: 100,
        member_assignment: %Kayrock.MemberAssignment{
          partition_assignments: []
        }
      }

      legacy_response = Adapter.sync_group_response(kayrock_response)

      assert %SyncGroupResponse{error_code: :no_error} = legacy_response
    end

    test "converts V1 response with zero throttle_time_ms" do
      kayrock_response = %SyncGroup.V1.Response{
        error_code: 0,
        throttle_time_ms: 0,
        member_assignment: %Kayrock.MemberAssignment{
          partition_assignments: []
        }
      }

      legacy_response = Adapter.sync_group_response(kayrock_response)

      assert %SyncGroupResponse{error_code: :no_error} = legacy_response
    end

    test "converts V1 response with partition assignments and throttle" do
      kayrock_response = %SyncGroup.V1.Response{
        error_code: 0,
        throttle_time_ms: 50,
        member_assignment: %Kayrock.MemberAssignment{
          partition_assignments: [
            %{topic: "topic-a", partitions: [0, 1]},
            %{topic: "topic-b", partitions: [2, 3, 4]}
          ]
        }
      }

      legacy_response = Adapter.sync_group_response(kayrock_response)

      assert %SyncGroupResponse{error_code: :no_error} = legacy_response
      assert length(legacy_response.assignments) == 2
    end

    test "converts V1 error response (unknown_member_id) with throttle" do
      kayrock_response = %SyncGroup.V1.Response{
        error_code: 25,
        throttle_time_ms: 50,
        member_assignment: %Kayrock.MemberAssignment{partition_assignments: []}
      }

      legacy_response = Adapter.sync_group_response(kayrock_response)

      assert %SyncGroupResponse{error_code: :unknown_member_id} = legacy_response
    end

    test "converts V1 error response (illegal_generation) with throttle" do
      kayrock_response = %SyncGroup.V1.Response{
        error_code: 22,
        throttle_time_ms: 75,
        member_assignment: %Kayrock.MemberAssignment{partition_assignments: []}
      }

      legacy_response = Adapter.sync_group_response(kayrock_response)

      assert %SyncGroupResponse{error_code: :illegal_generation} = legacy_response
    end

    test "converts V1 error response (rebalance_in_progress) with throttle" do
      kayrock_response = %SyncGroup.V1.Response{
        error_code: 27,
        throttle_time_ms: 200,
        member_assignment: %Kayrock.MemberAssignment{partition_assignments: []}
      }

      legacy_response = Adapter.sync_group_response(kayrock_response)

      assert %SyncGroupResponse{error_code: :rebalance_in_progress} = legacy_response
    end

    test "converts V1 error response (not_coordinator) with throttle" do
      kayrock_response = %SyncGroup.V1.Response{
        error_code: 16,
        throttle_time_ms: 10,
        member_assignment: %Kayrock.MemberAssignment{partition_assignments: []}
      }

      legacy_response = Adapter.sync_group_response(kayrock_response)

      assert %SyncGroupResponse{error_code: :not_coordinator} = legacy_response
    end

    test "converts V1 error response (coordinator_not_available) with throttle" do
      kayrock_response = %SyncGroup.V1.Response{
        error_code: 15,
        throttle_time_ms: 25,
        member_assignment: %Kayrock.MemberAssignment{partition_assignments: []}
      }

      legacy_response = Adapter.sync_group_response(kayrock_response)

      assert %SyncGroupResponse{error_code: :coordinator_not_available} = legacy_response
    end

    test "converts V1 error response (group_authorization_failed) with throttle" do
      kayrock_response = %SyncGroup.V1.Response{
        error_code: 30,
        throttle_time_ms: 30,
        member_assignment: %Kayrock.MemberAssignment{partition_assignments: []}
      }

      legacy_response = Adapter.sync_group_response(kayrock_response)

      assert %SyncGroupResponse{error_code: :group_authorization_failed} = legacy_response
    end
  end

  describe "round-trip conversion" do
    test "legacy request -> kayrock -> legacy response maintains structure" do
      # Start with legacy request
      legacy_request = %SyncGroupRequest{
        group_name: "test-group",
        member_id: "member-1",
        generation_id: 42,
        assignments: []
      }

      # Convert to Kayrock format
      {kayrock_request, consumer_group} = Adapter.sync_group_request(legacy_request)

      # Verify conversion
      assert consumer_group == "test-group"
      assert kayrock_request.group_id == "test-group"
      assert kayrock_request.member_id == "member-1"
      assert kayrock_request.generation_id == 42
      assert kayrock_request.group_assignment == []

      # Simulate successful Kayrock response (V0)
      kayrock_response_v0 = %SyncGroup.V0.Response{
        error_code: 0,
        member_assignment: %Kayrock.MemberAssignment{partition_assignments: []}
      }

      # Convert back to legacy
      legacy_response = Adapter.sync_group_response(kayrock_response_v0)

      assert %SyncGroupResponse{error_code: :no_error, assignments: []} = legacy_response
    end

    test "legacy request -> kayrock V1 -> legacy response maintains error semantics" do
      # Start with legacy request
      legacy_request = %SyncGroupRequest{
        group_name: "test-group",
        member_id: "member-1",
        generation_id: 42,
        assignments: []
      }

      # Convert to Kayrock format (would be V0 request)
      {_kayrock_request, _consumer_group} = Adapter.sync_group_request(legacy_request)

      # Simulate error Kayrock response (V1 with throttle)
      kayrock_response_v1 = %SyncGroup.V1.Response{
        error_code: 27,
        throttle_time_ms: 150,
        member_assignment: %Kayrock.MemberAssignment{partition_assignments: []}
      }

      # Convert back to legacy
      legacy_response = Adapter.sync_group_response(kayrock_response_v1)

      # Verify error code is preserved (throttle is ignored in legacy)
      assert %SyncGroupResponse{error_code: :rebalance_in_progress} = legacy_response
    end

    test "round-trip with partition assignments" do
      # Start with legacy request containing assignments
      legacy_request = %SyncGroupRequest{
        group_name: "test-group",
        member_id: "leader",
        generation_id: 1,
        assignments: [
          {"member-1", [{"topic-x", [0, 1]}]},
          {"member-2", [{"topic-y", [2]}]}
        ]
      }

      # Convert to Kayrock
      {kayrock_request, _consumer_group} = Adapter.sync_group_request(legacy_request)

      assert length(kayrock_request.group_assignment) == 2

      # Verify the Kayrock format
      [assignment1, assignment2] = kayrock_request.group_assignment
      assert assignment1.member_id == "member-1"
      assert assignment2.member_id == "member-2"

      # Simulate response with assignments
      kayrock_response = %SyncGroup.V0.Response{
        error_code: 0,
        member_assignment: %Kayrock.MemberAssignment{
          partition_assignments: [
            %{topic: "topic-x", partitions: [0, 1]}
          ]
        }
      }

      # Convert back to legacy
      legacy_response = Adapter.sync_group_response(kayrock_response)

      assert %SyncGroupResponse{error_code: :no_error} = legacy_response
      assert [{"topic-x", [0, 1]}] = legacy_response.assignments
    end
  end
end
