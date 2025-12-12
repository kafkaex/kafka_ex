defmodule KafkaEx.New.Adapter.JoinGroupTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Adapter
  alias KafkaEx.Protocol.JoinGroup.Request, as: JoinGroupRequest
  alias KafkaEx.Protocol.JoinGroup.Response, as: JoinGroupResponse
  alias Kayrock.JoinGroup

  describe "join_group_request/1 - legacy to new API" do
    test "converts legacy JoinGroupRequest to Kayrock V0 request" do
      legacy_request = %JoinGroupRequest{
        group_name: "test-consumer-group",
        member_id: "",
        topics: ["topic-1", "topic-2"],
        session_timeout: 30000
      }

      {kayrock_request, consumer_group} = Adapter.join_group_request(legacy_request)

      assert consumer_group == "test-consumer-group"

      assert kayrock_request == %JoinGroup.V0.Request{
               group_id: "test-consumer-group",
               member_id: "",
               session_timeout: 30000,
               protocol_type: "consumer",
               group_protocols: [
                 %{
                   protocol_name: "assign",
                   protocol_metadata: %Kayrock.GroupProtocolMetadata{
                     topics: ["topic-1", "topic-2"]
                   }
                 }
               ]
             }
    end

    test "handles empty member_id for initial join" do
      legacy_request = %JoinGroupRequest{
        group_name: "my-group",
        member_id: "",
        topics: ["topic-1"],
        session_timeout: 10000
      }

      {kayrock_request, consumer_group} = Adapter.join_group_request(legacy_request)

      assert consumer_group == "my-group"
      assert kayrock_request.member_id == ""
      assert kayrock_request.group_id == "my-group"
    end

    test "handles existing member_id for rejoin" do
      legacy_request = %JoinGroupRequest{
        group_name: "group",
        member_id: "consumer-12345-uuid",
        topics: ["topic-1"],
        session_timeout: 15000
      }

      {kayrock_request, _consumer_group} = Adapter.join_group_request(legacy_request)

      assert kayrock_request.member_id == "consumer-12345-uuid"
    end

    test "handles single topic" do
      legacy_request = %JoinGroupRequest{
        group_name: "group",
        member_id: "",
        topics: ["single-topic"],
        session_timeout: 30000
      }

      {kayrock_request, _consumer_group} = Adapter.join_group_request(legacy_request)

      [protocol] = kayrock_request.group_protocols
      assert protocol.protocol_metadata.topics == ["single-topic"]
    end

    test "handles multiple topics" do
      legacy_request = %JoinGroupRequest{
        group_name: "group",
        member_id: "",
        topics: ["topic-a", "topic-b", "topic-c"],
        session_timeout: 30000
      }

      {kayrock_request, _consumer_group} = Adapter.join_group_request(legacy_request)

      [protocol] = kayrock_request.group_protocols
      assert protocol.protocol_metadata.topics == ["topic-a", "topic-b", "topic-c"]
    end

    test "sets protocol_type to consumer" do
      legacy_request = %JoinGroupRequest{
        group_name: "group",
        member_id: "",
        topics: ["topic"],
        session_timeout: 30000
      }

      {kayrock_request, _consumer_group} = Adapter.join_group_request(legacy_request)

      assert kayrock_request.protocol_type == "consumer"
    end

    test "sets protocol_name to assign" do
      legacy_request = %JoinGroupRequest{
        group_name: "group",
        member_id: "",
        topics: ["topic"],
        session_timeout: 30000
      }

      {kayrock_request, _consumer_group} = Adapter.join_group_request(legacy_request)

      [protocol] = kayrock_request.group_protocols
      assert protocol.protocol_name == "assign"
    end
  end

  describe "join_group_response/1 - new to legacy API (V0)" do
    test "converts successful V0 response to legacy format" do
      kayrock_response = %JoinGroup.V0.Response{
        error_code: 0,
        generation_id: 1,
        group_protocol: "assign",
        leader_id: "consumer-123",
        member_id: "consumer-123",
        members: [
          %{member_id: "consumer-123", member_metadata: <<>>},
          %{member_id: "consumer-456", member_metadata: <<>>}
        ]
      }

      legacy_response = Adapter.join_group_response(kayrock_response)

      assert %JoinGroupResponse{
               error_code: :no_error,
               generation_id: 1,
               leader_id: "consumer-123",
               member_id: "consumer-123",
               members: ["consumer-123", "consumer-456"]
             } = legacy_response
    end

    test "converts V0 response for non-leader member" do
      kayrock_response = %JoinGroup.V0.Response{
        error_code: 0,
        generation_id: 5,
        group_protocol: "assign",
        leader_id: "leader-member",
        member_id: "follower-member",
        members: []
      }

      legacy_response = Adapter.join_group_response(kayrock_response)

      assert legacy_response.error_code == :no_error
      assert legacy_response.generation_id == 5
      assert legacy_response.leader_id == "leader-member"
      assert legacy_response.member_id == "follower-member"
      assert legacy_response.members == []
    end

    test "converts V0 error response (unknown_member_id)" do
      kayrock_response = %JoinGroup.V0.Response{
        error_code: 25,
        generation_id: -1,
        group_protocol: "",
        leader_id: "",
        member_id: "",
        members: []
      }

      legacy_response = Adapter.join_group_response(kayrock_response)

      assert %JoinGroupResponse{error_code: :unknown_member_id} = legacy_response
    end

    test "converts V0 error response (illegal_generation)" do
      kayrock_response = %JoinGroup.V0.Response{
        error_code: 22,
        generation_id: -1,
        group_protocol: "",
        leader_id: "",
        member_id: "",
        members: []
      }

      legacy_response = Adapter.join_group_response(kayrock_response)

      assert %JoinGroupResponse{error_code: :illegal_generation} = legacy_response
    end

    test "converts V0 error response (rebalance_in_progress)" do
      kayrock_response = %JoinGroup.V0.Response{
        error_code: 27,
        generation_id: -1,
        group_protocol: "",
        leader_id: "",
        member_id: "",
        members: []
      }

      legacy_response = Adapter.join_group_response(kayrock_response)

      assert %JoinGroupResponse{error_code: :rebalance_in_progress} = legacy_response
    end

    test "converts V0 error response (not_coordinator)" do
      kayrock_response = %JoinGroup.V0.Response{
        error_code: 16,
        generation_id: -1,
        group_protocol: "",
        leader_id: "",
        member_id: "",
        members: []
      }

      legacy_response = Adapter.join_group_response(kayrock_response)

      assert %JoinGroupResponse{error_code: :not_coordinator} = legacy_response
    end

    test "converts V0 error response (coordinator_not_available)" do
      kayrock_response = %JoinGroup.V0.Response{
        error_code: 15,
        generation_id: -1,
        group_protocol: "",
        leader_id: "",
        member_id: "",
        members: []
      }

      legacy_response = Adapter.join_group_response(kayrock_response)

      assert %JoinGroupResponse{error_code: :coordinator_not_available} = legacy_response
    end

    test "extracts only member_ids from members list" do
      kayrock_response = %JoinGroup.V0.Response{
        error_code: 0,
        generation_id: 1,
        group_protocol: "assign",
        leader_id: "leader",
        member_id: "leader",
        members: [
          %{member_id: "member-1", member_metadata: <<1, 2, 3>>},
          %{member_id: "member-2", member_metadata: <<4, 5, 6>>},
          %{member_id: "member-3", member_metadata: <<7, 8, 9>>}
        ]
      }

      legacy_response = Adapter.join_group_response(kayrock_response)

      assert legacy_response.members == ["member-1", "member-2", "member-3"]
    end
  end

  describe "round-trip conversion" do
    test "legacy request -> kayrock -> legacy response maintains structure" do
      # Start with legacy request
      legacy_request = %JoinGroupRequest{
        group_name: "test-group",
        member_id: "",
        topics: ["topic-1"],
        session_timeout: 30000
      }

      # Convert to Kayrock format
      {kayrock_request, consumer_group} = Adapter.join_group_request(legacy_request)

      # Verify conversion
      assert consumer_group == "test-group"
      assert kayrock_request.group_id == "test-group"
      assert kayrock_request.member_id == ""
      assert kayrock_request.session_timeout == 30000

      # Simulate successful Kayrock response
      kayrock_response = %JoinGroup.V0.Response{
        error_code: 0,
        generation_id: 1,
        group_protocol: "assign",
        leader_id: "assigned-member-id",
        member_id: "assigned-member-id",
        members: [%{member_id: "assigned-member-id", member_metadata: <<>>}]
      }

      # Convert back to legacy
      legacy_response = Adapter.join_group_response(kayrock_response)

      assert %JoinGroupResponse{
               error_code: :no_error,
               generation_id: 1,
               member_id: "assigned-member-id"
             } = legacy_response
    end

    test "Response.leader?/1 works correctly with converted response" do
      # Leader response
      leader_response =
        Adapter.join_group_response(%JoinGroup.V0.Response{
          error_code: 0,
          generation_id: 1,
          group_protocol: "assign",
          leader_id: "member-1",
          member_id: "member-1",
          members: []
        })

      assert JoinGroupResponse.leader?(leader_response)

      # Follower response
      follower_response =
        Adapter.join_group_response(%JoinGroup.V0.Response{
          error_code: 0,
          generation_id: 1,
          group_protocol: "assign",
          leader_id: "leader",
          member_id: "follower",
          members: []
        })

      refute JoinGroupResponse.leader?(follower_response)
    end
  end
end
