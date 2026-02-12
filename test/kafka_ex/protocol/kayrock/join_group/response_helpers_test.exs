defmodule KafkaEx.Protocol.Kayrock.JoinGroup.ResponseHelpersTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Protocol.Kayrock.JoinGroup.ResponseHelpers
  alias KafkaEx.Client.Error
  alias KafkaEx.Messages.JoinGroup

  describe "parse_response/2" do
    test "parses successful response without throttle time (V0/V1)" do
      response = %{
        error_code: 0,
        generation_id: 5,
        protocol_name: "range",
        leader: "leader-member",
        member_id: "member-123",
        members: []
      }

      throttle_extractor = fn _resp -> nil end

      assert {:ok, %JoinGroup{} = result} = ResponseHelpers.parse_response(response, throttle_extractor)
      assert result.generation_id == 5
      assert result.group_protocol == "range"
      assert result.leader_id == "leader-member"
      assert result.member_id == "member-123"
      assert result.throttle_time_ms == nil
      assert result.members == []
    end

    test "parses successful response with throttle time (V2)" do
      response = %{
        error_code: 0,
        generation_id: 3,
        protocol_name: "roundrobin",
        leader: "leader",
        member_id: "me",
        members: [],
        throttle_time_ms: 100
      }

      throttle_extractor = fn resp -> resp.throttle_time_ms end

      assert {:ok, %JoinGroup{} = result} = ResponseHelpers.parse_response(response, throttle_extractor)
      assert result.throttle_time_ms == 100
    end

    test "parses response with members" do
      response = %{
        error_code: 0,
        generation_id: 1,
        protocol_name: "range",
        leader: "member-1",
        member_id: "member-1",
        members: [
          %{member_id: "member-1", metadata: <<1, 2, 3>>},
          %{member_id: "member-2", metadata: <<4, 5, 6>>}
        ]
      }

      throttle_extractor = fn _resp -> nil end

      assert {:ok, %JoinGroup{} = result} = ResponseHelpers.parse_response(response, throttle_extractor)
      assert length(result.members) == 2

      [m1, m2] = result.members
      assert %JoinGroup.Member{member_id: "member-1", member_metadata: <<1, 2, 3>>} = m1
      assert %JoinGroup.Member{member_id: "member-2", member_metadata: <<4, 5, 6>>} = m2
    end

    test "returns error for non-zero error code" do
      response = %{
        error_code: 25,
        generation_id: -1,
        protocol_name: "",
        leader: "",
        member_id: "",
        members: []
      }

      throttle_extractor = fn _resp -> nil end

      assert {:error, %Error{}} = ResponseHelpers.parse_response(response, throttle_extractor)
    end

    test "handles nil members" do
      response = %{
        error_code: 0,
        generation_id: 1,
        protocol_name: "range",
        leader: "leader",
        member_id: "member",
        members: nil
      }

      throttle_extractor = fn _resp -> nil end

      assert {:ok, %JoinGroup{} = result} = ResponseHelpers.parse_response(response, throttle_extractor)
      assert result.members == []
    end
  end
end
