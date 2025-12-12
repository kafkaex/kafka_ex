defmodule KafkaEx.New.Protocols.Kayrock.JoinGroup.ResponseTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Protocols.Kayrock.JoinGroup
  alias KafkaEx.New.Kafka.JoinGroup, as: JoinGroupStruct

  describe "V0 Response implementation" do
    test "parses successful response with no error and empty members" do
      response = %Kayrock.JoinGroup.V0.Response{
        error_code: 0,
        generation_id: 5,
        group_protocol: "assign",
        leader_id: "member-123",
        member_id: "member-123",
        members: []
      }

      assert {:ok, result} = JoinGroup.Response.parse_response(response)
      assert result.throttle_time_ms == nil
      assert result.generation_id == 5
      assert result.group_protocol == "assign"
      assert result.leader_id == "member-123"
      assert result.member_id == "member-123"
      assert result.members == []
    end

    test "parses successful response with multiple members" do
      members = [
        %{member_id: "member-1", member_metadata: <<0, 1, 2>>},
        %{member_id: "member-2", member_metadata: <<3, 4, 5>>},
        %{member_id: "member-3", member_metadata: <<6, 7, 8>>}
      ]

      response = %Kayrock.JoinGroup.V0.Response{
        error_code: 0,
        generation_id: 10,
        group_protocol: "range",
        leader_id: "member-1",
        member_id: "member-1",
        members: members
      }

      assert {:ok, result} = JoinGroup.Response.parse_response(response)
      assert length(result.members) == 3

      [m1, m2, m3] = result.members
      assert %JoinGroupStruct.Member{member_id: "member-1", member_metadata: <<0, 1, 2>>} = m1
      assert %JoinGroupStruct.Member{member_id: "member-2", member_metadata: <<3, 4, 5>>} = m2
      assert %JoinGroupStruct.Member{member_id: "member-3", member_metadata: <<6, 7, 8>>} = m3
    end

    test "leader? returns true when member_id equals leader_id" do
      response = %Kayrock.JoinGroup.V0.Response{
        error_code: 0,
        generation_id: 1,
        group_protocol: "assign",
        leader_id: "leader-member",
        member_id: "leader-member",
        members: []
      }

      assert {:ok, result} = JoinGroup.Response.parse_response(response)
      assert JoinGroupStruct.leader?(result) == true
    end

    test "leader? returns false when member_id does not equal leader_id" do
      response = %Kayrock.JoinGroup.V0.Response{
        error_code: 0,
        generation_id: 1,
        group_protocol: "assign",
        leader_id: "leader-member",
        member_id: "follower-member",
        members: []
      }

      assert {:ok, result} = JoinGroup.Response.parse_response(response)
      assert JoinGroupStruct.leader?(result) == false
    end

    test "parses error response with unknown_member_id" do
      response = %Kayrock.JoinGroup.V0.Response{
        error_code: 25,
        generation_id: 0,
        group_protocol: "",
        leader_id: "",
        member_id: "",
        members: []
      }

      assert {:error, error} = JoinGroup.Response.parse_response(response)
      assert error.error == :unknown_member_id
    end

    test "parses error response with illegal_generation" do
      response = %Kayrock.JoinGroup.V0.Response{
        error_code: 22,
        generation_id: 0,
        group_protocol: "",
        leader_id: "",
        member_id: "",
        members: []
      }

      assert {:error, error} = JoinGroup.Response.parse_response(response)
      assert error.error == :illegal_generation
    end

    test "parses error response with rebalance_in_progress" do
      response = %Kayrock.JoinGroup.V0.Response{
        error_code: 27,
        generation_id: 0,
        group_protocol: "",
        leader_id: "",
        member_id: "",
        members: []
      }

      assert {:error, error} = JoinGroup.Response.parse_response(response)
      assert error.error == :rebalance_in_progress
    end

    test "parses error response with group_authorization_failed" do
      response = %Kayrock.JoinGroup.V0.Response{
        error_code: 30,
        generation_id: 0,
        group_protocol: "",
        leader_id: "",
        member_id: "",
        members: []
      }

      assert {:error, error} = JoinGroup.Response.parse_response(response)
      assert error.error == :group_authorization_failed
    end

    test "handles nil members list" do
      response = %Kayrock.JoinGroup.V0.Response{
        error_code: 0,
        generation_id: 5,
        group_protocol: "assign",
        leader_id: "member-123",
        member_id: "member-123",
        members: nil
      }

      assert {:ok, result} = JoinGroup.Response.parse_response(response)
      assert result.members == []
    end

    test "handles generation_id zero (initial join)" do
      response = %Kayrock.JoinGroup.V0.Response{
        error_code: 0,
        generation_id: 0,
        group_protocol: "assign",
        leader_id: "member-1",
        member_id: "member-1",
        members: []
      }

      assert {:ok, result} = JoinGroup.Response.parse_response(response)
      assert result.generation_id == 0
    end

    test "V0 response never includes throttle_time_ms" do
      response = %Kayrock.JoinGroup.V0.Response{
        error_code: 0,
        generation_id: 5,
        group_protocol: "assign",
        leader_id: "member-123",
        member_id: "member-123",
        members: []
      }

      assert {:ok, result} = JoinGroup.Response.parse_response(response)
      assert result.throttle_time_ms == nil
    end
  end

  describe "V1 Response implementation" do
    test "parses successful response with no error" do
      members = [
        %{member_id: "member-1", member_metadata: <<0, 1, 2>>}
      ]

      response = %Kayrock.JoinGroup.V1.Response{
        error_code: 0,
        generation_id: 5,
        group_protocol: "assign",
        leader_id: "member-1",
        member_id: "member-1",
        members: members
      }

      assert {:ok, result} = JoinGroup.Response.parse_response(response)
      assert result.throttle_time_ms == nil
      assert result.generation_id == 5
      assert result.group_protocol == "assign"
      assert result.leader_id == "member-1"
      assert result.member_id == "member-1"
      assert length(result.members) == 1
    end

    test "parses response with multiple members" do
      members = [
        %{member_id: "member-1", member_metadata: <<0, 1>>},
        %{member_id: "member-2", member_metadata: <<2, 3>>}
      ]

      response = %Kayrock.JoinGroup.V1.Response{
        error_code: 0,
        generation_id: 10,
        group_protocol: "roundrobin",
        leader_id: "member-1",
        member_id: "member-2",
        members: members
      }

      assert {:ok, result} = JoinGroup.Response.parse_response(response)
      assert length(result.members) == 2
      assert JoinGroupStruct.leader?(result) == false
    end

    test "parses error responses" do
      response = %Kayrock.JoinGroup.V1.Response{
        error_code: 25,
        generation_id: 0,
        group_protocol: "",
        leader_id: "",
        member_id: "",
        members: []
      }

      assert {:error, error} = JoinGroup.Response.parse_response(response)
      assert error.error == :unknown_member_id
    end

    test "V1 response structure matches V0 (no throttle_time_ms)" do
      response = %Kayrock.JoinGroup.V1.Response{
        error_code: 0,
        generation_id: 5,
        group_protocol: "assign",
        leader_id: "member-123",
        member_id: "member-123",
        members: []
      }

      assert {:ok, result} = JoinGroup.Response.parse_response(response)
      assert result.throttle_time_ms == nil
    end
  end

  describe "V2 Response implementation" do
    test "parses successful response with throttle_time_ms" do
      members = [
        %{member_id: "member-1", member_metadata: <<0, 1, 2>>}
      ]

      response = %Kayrock.JoinGroup.V2.Response{
        throttle_time_ms: 100,
        error_code: 0,
        generation_id: 5,
        group_protocol: "assign",
        leader_id: "member-1",
        member_id: "member-1",
        members: members
      }

      assert {:ok, result} = JoinGroup.Response.parse_response(response)
      assert result.throttle_time_ms == 100
      assert result.generation_id == 5
      assert result.group_protocol == "assign"
      assert result.leader_id == "member-1"
      assert result.member_id == "member-1"
      assert length(result.members) == 1
    end

    test "parses response with zero throttle_time_ms" do
      response = %Kayrock.JoinGroup.V2.Response{
        throttle_time_ms: 0,
        error_code: 0,
        generation_id: 10,
        group_protocol: "range",
        leader_id: "member-123",
        member_id: "member-123",
        members: []
      }

      assert {:ok, result} = JoinGroup.Response.parse_response(response)
      assert result.throttle_time_ms == 0
    end

    test "parses response with large throttle_time_ms" do
      response = %Kayrock.JoinGroup.V2.Response{
        throttle_time_ms: 5000,
        error_code: 0,
        generation_id: 1,
        group_protocol: "assign",
        leader_id: "member-1",
        member_id: "member-1",
        members: []
      }

      assert {:ok, result} = JoinGroup.Response.parse_response(response)
      assert result.throttle_time_ms == 5000
    end

    test "parses response with multiple members" do
      members = [
        %{member_id: "member-1", member_metadata: <<0, 1, 2>>},
        %{member_id: "member-2", member_metadata: <<3, 4, 5>>},
        %{member_id: "member-3", member_metadata: <<6, 7, 8>>}
      ]

      response = %Kayrock.JoinGroup.V2.Response{
        throttle_time_ms: 50,
        error_code: 0,
        generation_id: 15,
        group_protocol: "roundrobin",
        leader_id: "member-1",
        member_id: "member-2",
        members: members
      }

      assert {:ok, result} = JoinGroup.Response.parse_response(response)
      assert result.throttle_time_ms == 50
      assert length(result.members) == 3
      assert JoinGroupStruct.leader?(result) == false
    end

    test "parses error response with throttle_time_ms" do
      response = %Kayrock.JoinGroup.V2.Response{
        throttle_time_ms: 200,
        error_code: 25,
        generation_id: 0,
        group_protocol: "",
        leader_id: "",
        member_id: "",
        members: []
      }

      assert {:error, error} = JoinGroup.Response.parse_response(response)
      assert error.error == :unknown_member_id
    end

    test "parses rebalance_in_progress error" do
      response = %Kayrock.JoinGroup.V2.Response{
        throttle_time_ms: 0,
        error_code: 27,
        generation_id: 0,
        group_protocol: "",
        leader_id: "",
        member_id: "",
        members: []
      }

      assert {:error, error} = JoinGroup.Response.parse_response(response)
      assert error.error == :rebalance_in_progress
    end

    test "handles nil members list in V2" do
      response = %Kayrock.JoinGroup.V2.Response{
        throttle_time_ms: 100,
        error_code: 0,
        generation_id: 5,
        group_protocol: "assign",
        leader_id: "member-123",
        member_id: "member-123",
        members: nil
      }

      assert {:ok, result} = JoinGroup.Response.parse_response(response)
      assert result.members == []
    end

    test "V2 is the first version with throttle_time_ms" do
      response = %Kayrock.JoinGroup.V2.Response{
        throttle_time_ms: 123,
        error_code: 0,
        generation_id: 1,
        group_protocol: "assign",
        leader_id: "member-1",
        member_id: "member-1",
        members: []
      }

      assert {:ok, result} = JoinGroup.Response.parse_response(response)
      assert result.throttle_time_ms == 123
    end
  end
end
