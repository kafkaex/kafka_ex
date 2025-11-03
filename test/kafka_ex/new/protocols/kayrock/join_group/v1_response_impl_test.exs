defmodule KafkaEx.New.Protocols.Kayrock.JoinGroup.V1ResponseImplTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Protocols.Kayrock.JoinGroup
  alias KafkaEx.New.Structs.JoinGroup, as: JoinGroupStruct

  describe "parse_response/1 for V1" do
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
      # V1 still doesn't have throttle_time_ms
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
end
