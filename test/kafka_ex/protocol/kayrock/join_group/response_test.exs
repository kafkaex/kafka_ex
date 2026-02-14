defmodule KafkaEx.Protocol.Kayrock.JoinGroup.ResponseTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Protocol.Kayrock.JoinGroup
  alias KafkaEx.Messages.JoinGroup, as: JoinGroupStruct

  describe "V0 Response implementation" do
    test "parses successful response with no error and empty members" do
      response = %Kayrock.JoinGroup.V0.Response{
        error_code: 0,
        generation_id: 5,
        protocol_name: "assign",
        leader: "member-123",
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
        %{member_id: "member-1", metadata: <<0, 1, 2>>},
        %{member_id: "member-2", metadata: <<3, 4, 5>>},
        %{member_id: "member-3", metadata: <<6, 7, 8>>}
      ]

      response = %Kayrock.JoinGroup.V0.Response{
        error_code: 0,
        generation_id: 10,
        protocol_name: "range",
        leader: "member-1",
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
        protocol_name: "assign",
        leader: "leader-member",
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
        protocol_name: "assign",
        leader: "leader-member",
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
        protocol_name: "",
        leader: "",
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
        protocol_name: "",
        leader: "",
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
        protocol_name: "",
        leader: "",
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
        protocol_name: "",
        leader: "",
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
        protocol_name: "assign",
        leader: "member-123",
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
        protocol_name: "assign",
        leader: "member-1",
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
        protocol_name: "assign",
        leader: "member-123",
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
        %{member_id: "member-1", metadata: <<0, 1, 2>>}
      ]

      response = %Kayrock.JoinGroup.V1.Response{
        error_code: 0,
        generation_id: 5,
        protocol_name: "assign",
        leader: "member-1",
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
        %{member_id: "member-1", metadata: <<0, 1>>},
        %{member_id: "member-2", metadata: <<2, 3>>}
      ]

      response = %Kayrock.JoinGroup.V1.Response{
        error_code: 0,
        generation_id: 10,
        protocol_name: "roundrobin",
        leader: "member-1",
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
        protocol_name: "",
        leader: "",
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
        protocol_name: "assign",
        leader: "member-123",
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
        %{member_id: "member-1", metadata: <<0, 1, 2>>}
      ]

      response = %Kayrock.JoinGroup.V2.Response{
        throttle_time_ms: 100,
        error_code: 0,
        generation_id: 5,
        protocol_name: "assign",
        leader: "member-1",
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
        protocol_name: "range",
        leader: "member-123",
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
        protocol_name: "assign",
        leader: "member-1",
        member_id: "member-1",
        members: []
      }

      assert {:ok, result} = JoinGroup.Response.parse_response(response)
      assert result.throttle_time_ms == 5000
    end

    test "parses response with multiple members" do
      members = [
        %{member_id: "member-1", metadata: <<0, 1, 2>>},
        %{member_id: "member-2", metadata: <<3, 4, 5>>},
        %{member_id: "member-3", metadata: <<6, 7, 8>>}
      ]

      response = %Kayrock.JoinGroup.V2.Response{
        throttle_time_ms: 50,
        error_code: 0,
        generation_id: 15,
        protocol_name: "roundrobin",
        leader: "member-1",
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
        protocol_name: "",
        leader: "",
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
        protocol_name: "",
        leader: "",
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
        protocol_name: "assign",
        leader: "member-123",
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
        protocol_name: "assign",
        leader: "member-1",
        member_id: "member-1",
        members: []
      }

      assert {:ok, result} = JoinGroup.Response.parse_response(response)
      assert result.throttle_time_ms == 123
    end
  end

  describe "V3 Response implementation" do
    test "parses successful response with throttle_time_ms" do
      response = %Kayrock.JoinGroup.V3.Response{
        throttle_time_ms: 150,
        error_code: 0,
        generation_id: 7,
        protocol_name: "assign",
        leader: "member-1",
        member_id: "member-1",
        members: [
          %{member_id: "member-1", metadata: <<0, 1, 2>>}
        ]
      }

      assert {:ok, result} = JoinGroup.Response.parse_response(response)
      assert result.throttle_time_ms == 150
      assert result.generation_id == 7
      assert result.group_protocol == "assign"
      assert result.leader_id == "member-1"
      assert result.member_id == "member-1"
      assert length(result.members) == 1
    end

    test "parses error response" do
      response = %Kayrock.JoinGroup.V3.Response{
        throttle_time_ms: 0,
        error_code: 25,
        generation_id: 0,
        protocol_name: "",
        leader: "",
        member_id: "",
        members: []
      }

      assert {:error, error} = JoinGroup.Response.parse_response(response)
      assert error.error == :unknown_member_id
    end
  end

  describe "V4 Response implementation" do
    test "parses successful response with throttle_time_ms" do
      response = %Kayrock.JoinGroup.V4.Response{
        throttle_time_ms: 200,
        error_code: 0,
        generation_id: 12,
        protocol_name: "roundrobin",
        leader: "leader-member",
        member_id: "follower-member",
        members: [
          %{member_id: "leader-member", metadata: <<1, 2>>},
          %{member_id: "follower-member", metadata: <<3, 4>>}
        ]
      }

      assert {:ok, result} = JoinGroup.Response.parse_response(response)
      assert result.throttle_time_ms == 200
      assert result.generation_id == 12
      assert result.group_protocol == "roundrobin"
      assert result.leader_id == "leader-member"
      assert result.member_id == "follower-member"
      assert length(result.members) == 2
      assert JoinGroupStruct.leader?(result) == false
    end

    test "handles nil members list" do
      response = %Kayrock.JoinGroup.V4.Response{
        throttle_time_ms: 0,
        error_code: 0,
        generation_id: 1,
        protocol_name: "assign",
        leader: "m1",
        member_id: "m1",
        members: nil
      }

      assert {:ok, result} = JoinGroup.Response.parse_response(response)
      assert result.members == []
    end
  end

  describe "V5 Response implementation" do
    test "parses successful response (group_instance_id in members silently ignored)" do
      # V5 members have group_instance_id but our extract_members only maps member_id + metadata
      response = %Kayrock.JoinGroup.V5.Response{
        throttle_time_ms: 50,
        error_code: 0,
        generation_id: 20,
        protocol_name: "assign",
        leader: "member-1",
        member_id: "member-1",
        members: [
          %{member_id: "member-1", group_instance_id: "instance-1", metadata: <<10, 20>>},
          %{member_id: "member-2", group_instance_id: nil, metadata: <<30, 40>>}
        ]
      }

      assert {:ok, result} = JoinGroup.Response.parse_response(response)
      assert result.throttle_time_ms == 50
      assert result.generation_id == 20
      assert result.group_protocol == "assign"
      assert result.leader_id == "member-1"
      assert length(result.members) == 2

      [m1, m2] = result.members
      assert %JoinGroupStruct.Member{member_id: "member-1", member_metadata: <<10, 20>>} = m1
      assert %JoinGroupStruct.Member{member_id: "member-2", member_metadata: <<30, 40>>} = m2
    end

    test "parses error response with rebalance_in_progress" do
      response = %Kayrock.JoinGroup.V5.Response{
        throttle_time_ms: 0,
        error_code: 27,
        generation_id: 0,
        protocol_name: "",
        leader: "",
        member_id: "",
        members: []
      }

      assert {:error, error} = JoinGroup.Response.parse_response(response)
      assert error.error == :rebalance_in_progress
    end

    test "handles empty members with group_instance_id schema" do
      response = %Kayrock.JoinGroup.V5.Response{
        throttle_time_ms: 0,
        error_code: 0,
        generation_id: 1,
        protocol_name: "assign",
        leader: "m1",
        member_id: "m1",
        members: []
      }

      assert {:ok, result} = JoinGroup.Response.parse_response(response)
      assert result.members == []
    end
  end

  describe "V6 Response implementation (FLEX)" do
    test "parses successful response (compact encoding handled by Kayrock)" do
      response = %Kayrock.JoinGroup.V6.Response{
        throttle_time_ms: 75,
        error_code: 0,
        generation_id: 25,
        protocol_name: "assign",
        leader: "member-1",
        member_id: "member-1",
        members: [
          %{
            member_id: "member-1",
            group_instance_id: "instance-1",
            metadata: <<10, 20>>,
            tagged_fields: []
          }
        ],
        tagged_fields: []
      }

      assert {:ok, result} = JoinGroup.Response.parse_response(response)
      assert result.throttle_time_ms == 75
      assert result.generation_id == 25
      assert result.group_protocol == "assign"
      assert result.leader_id == "member-1"
      assert length(result.members) == 1

      [m1] = result.members
      assert %JoinGroupStruct.Member{member_id: "member-1", member_metadata: <<10, 20>>} = m1
    end

    test "parses error response" do
      response = %Kayrock.JoinGroup.V6.Response{
        throttle_time_ms: 0,
        error_code: 25,
        generation_id: 0,
        protocol_name: "",
        leader: "",
        member_id: "",
        members: [],
        tagged_fields: []
      }

      assert {:error, error} = JoinGroup.Response.parse_response(response)
      assert error.error == :unknown_member_id
    end

    test "handles nil members list" do
      response = %Kayrock.JoinGroup.V6.Response{
        throttle_time_ms: 0,
        error_code: 0,
        generation_id: 1,
        protocol_name: "assign",
        leader: "m1",
        member_id: "m1",
        members: nil,
        tagged_fields: []
      }

      assert {:ok, result} = JoinGroup.Response.parse_response(response)
      assert result.members == []
    end
  end

  describe "Cross-version response consistency (V2-V6)" do
    @success_fields %{
      error_code: 0,
      generation_id: 42,
      protocol_name: "assign",
      leader: "leader-member",
      member_id: "leader-member",
      members: [
        %{member_id: "leader-member", metadata: <<1, 2, 3>>}
      ]
    }

    @v2_v4_response_structs [
      {struct(Kayrock.JoinGroup.V2.Response, Map.put(@success_fields, :throttle_time_ms, 100)), "V2"},
      {struct(Kayrock.JoinGroup.V3.Response, Map.put(@success_fields, :throttle_time_ms, 100)), "V3"},
      {struct(Kayrock.JoinGroup.V4.Response, Map.put(@success_fields, :throttle_time_ms, 100)), "V4"}
    ]

    test "V2-V4 produce identical domain results" do
      results =
        Enum.map(@v2_v4_response_structs, fn {response, _label} ->
          {:ok, result} = JoinGroup.Response.parse_response(response)
          result
        end)

      for result <- results do
        assert result.throttle_time_ms == 100
        assert result.generation_id == 42
        assert result.group_protocol == "assign"
        assert result.leader_id == "leader-member"
        assert result.member_id == "leader-member"
        assert length(result.members) == 1
      end
    end

    test "V5 and V6 produce identical domain results (group_instance_id ignored)" do
      v5_members = [
        %{member_id: "leader-member", group_instance_id: "inst-1", metadata: <<1, 2, 3>>}
      ]

      v6_members = [
        %{
          member_id: "leader-member",
          group_instance_id: "inst-1",
          metadata: <<1, 2, 3>>,
          tagged_fields: []
        }
      ]

      v5_response = %Kayrock.JoinGroup.V5.Response{
        throttle_time_ms: 100,
        error_code: 0,
        generation_id: 42,
        protocol_name: "assign",
        leader: "leader-member",
        member_id: "leader-member",
        members: v5_members
      }

      v6_response = %Kayrock.JoinGroup.V6.Response{
        throttle_time_ms: 100,
        error_code: 0,
        generation_id: 42,
        protocol_name: "assign",
        leader: "leader-member",
        member_id: "leader-member",
        members: v6_members,
        tagged_fields: []
      }

      {:ok, v5_result} = JoinGroup.Response.parse_response(v5_response)
      {:ok, v6_result} = JoinGroup.Response.parse_response(v6_response)

      assert v5_result.throttle_time_ms == v6_result.throttle_time_ms
      assert v5_result.generation_id == v6_result.generation_id
      assert v5_result.group_protocol == v6_result.group_protocol
      assert v5_result.leader_id == v6_result.leader_id
      assert v5_result.member_id == v6_result.member_id
      assert v5_result.members == v6_result.members
    end
  end

  describe "Any fallback Response implementation" do
    test "routes V2+-like map to throttle_time path" do
      response = %{
        throttle_time_ms: 250,
        error_code: 0,
        generation_id: 5,
        protocol_name: "assign",
        leader: "member-1",
        member_id: "member-1",
        members: []
      }

      assert {:ok, result} = JoinGroup.Response.parse_response(response)
      assert result.throttle_time_ms == 250
      assert result.generation_id == 5
    end

    test "routes V0/V1-like map to nil throttle_time path" do
      response = %{
        error_code: 0,
        generation_id: 3,
        protocol_name: "range",
        leader: "m1",
        member_id: "m1",
        members: []
      }

      assert {:ok, result} = JoinGroup.Response.parse_response(response)
      assert result.throttle_time_ms == nil
      assert result.generation_id == 3
    end

    test "handles error response via Any path" do
      response = %{
        throttle_time_ms: 0,
        error_code: 22,
        generation_id: 0,
        protocol_name: "",
        leader: "",
        member_id: "",
        members: []
      }

      assert {:error, error} = JoinGroup.Response.parse_response(response)
      assert error.error == :illegal_generation
    end

    test "handles V5+ members with group_instance_id via Any path" do
      response = %{
        throttle_time_ms: 50,
        error_code: 0,
        generation_id: 10,
        protocol_name: "assign",
        leader: "m1",
        member_id: "m1",
        members: [
          %{member_id: "m1", group_instance_id: "inst-1", metadata: <<1, 2>>}
        ]
      }

      assert {:ok, result} = JoinGroup.Response.parse_response(response)
      assert length(result.members) == 1
      [m1] = result.members
      assert m1.member_id == "m1"
      assert m1.member_metadata == <<1, 2>>
    end
  end
end
