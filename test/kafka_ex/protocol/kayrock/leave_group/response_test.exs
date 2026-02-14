defmodule KafkaEx.Protocol.Kayrock.LeaveGroup.ResponseTest do
  use ExUnit.Case, async: true

  # NOTE: ResponseHelpers.parse_v0_response/1, parse_v1_v2_response/1, and
  # parse_v3_plus_response/1 are tested in response_helpers_test.exs.
  # This file tests protocol dispatch: LeaveGroup.Response.parse_response/1

  alias KafkaEx.Protocol.Kayrock.LeaveGroup
  alias KafkaEx.Messages.LeaveGroup, as: LeaveGroupStruct

  # ---- V0 Response ----

  describe "V0 Response implementation" do
    test "parses successful response with no error" do
      response = %Kayrock.LeaveGroup.V0.Response{
        error_code: 0
      }

      assert {:ok, :no_error} = LeaveGroup.Response.parse_response(response)
    end

    test "parses error response with unknown_member_id" do
      response = %Kayrock.LeaveGroup.V0.Response{
        error_code: 25
      }

      assert {:error, error} = LeaveGroup.Response.parse_response(response)
      assert error.error == :unknown_member_id
      assert error.metadata == %{}
    end

    test "parses error response with group_id_not_found" do
      response = %Kayrock.LeaveGroup.V0.Response{
        error_code: 69
      }

      assert {:error, error} = LeaveGroup.Response.parse_response(response)
      assert error.error == :group_id_not_found
    end

    test "parses error response with rebalance_in_progress" do
      response = %Kayrock.LeaveGroup.V0.Response{
        error_code: 27
      }

      assert {:error, error} = LeaveGroup.Response.parse_response(response)
      assert error.error == :rebalance_in_progress
    end

    test "parses error response with coordinator_not_available" do
      response = %Kayrock.LeaveGroup.V0.Response{
        error_code: 15
      }

      assert {:error, error} = LeaveGroup.Response.parse_response(response)
      assert error.error == :coordinator_not_available
    end

    test "parses error response with not_coordinator" do
      response = %Kayrock.LeaveGroup.V0.Response{
        error_code: 16
      }

      assert {:error, error} = LeaveGroup.Response.parse_response(response)
      assert error.error == :not_coordinator
    end

    test "parses error response with group_authorization_failed" do
      response = %Kayrock.LeaveGroup.V0.Response{
        error_code: 30
      }

      assert {:error, error} = LeaveGroup.Response.parse_response(response)
      assert error.error == :group_authorization_failed
    end

    test "error struct has empty metadata" do
      response = %Kayrock.LeaveGroup.V0.Response{
        error_code: 27
      }

      {:error, error} = LeaveGroup.Response.parse_response(response)

      assert error.metadata == %{}
    end
  end

  # ---- V1 Response ----

  describe "V1 Response implementation" do
    test "parses successful response with throttle_time_ms" do
      response = %Kayrock.LeaveGroup.V1.Response{
        error_code: 0,
        throttle_time_ms: 0
      }

      assert {:ok, leave_group} = LeaveGroup.Response.parse_response(response)
      assert %LeaveGroupStruct{throttle_time_ms: 0} = leave_group
    end

    test "parses successful response with non-zero throttle time" do
      response = %Kayrock.LeaveGroup.V1.Response{
        error_code: 0,
        throttle_time_ms: 100
      }

      assert {:ok, leave_group} = LeaveGroup.Response.parse_response(response)
      assert leave_group.throttle_time_ms == 100
    end

    test "parses successful response with large throttle time" do
      response = %Kayrock.LeaveGroup.V1.Response{
        error_code: 0,
        throttle_time_ms: 5000
      }

      assert {:ok, leave_group} = LeaveGroup.Response.parse_response(response)
      assert leave_group.throttle_time_ms == 5000
    end

    test "parses error response with unknown_member_id" do
      response = %Kayrock.LeaveGroup.V1.Response{
        error_code: 25
      }

      assert {:error, error} = LeaveGroup.Response.parse_response(response)
      assert error.error == :unknown_member_id
    end

    test "parses error response with group_id_not_found" do
      response = %Kayrock.LeaveGroup.V1.Response{
        error_code: 69
      }

      assert {:error, error} = LeaveGroup.Response.parse_response(response)
      assert error.error == :group_id_not_found
    end

    test "parses error response with rebalance_in_progress" do
      response = %Kayrock.LeaveGroup.V1.Response{
        error_code: 27
      }

      assert {:error, error} = LeaveGroup.Response.parse_response(response)
      assert error.error == :rebalance_in_progress
    end

    test "error response ignores throttle_time_ms field" do
      response = %Kayrock.LeaveGroup.V1.Response{
        error_code: 25,
        throttle_time_ms: 100
      }

      assert {:error, error} = LeaveGroup.Response.parse_response(response)
      assert error.error == :unknown_member_id
      # Throttle time is not included in error response
      refute Map.has_key?(error, :throttle_time_ms)
    end
  end

  # ---- V2 Response ----

  describe "V2 Response implementation" do
    test "parses successful response with throttle_time_ms" do
      response = %Kayrock.LeaveGroup.V2.Response{
        error_code: 0,
        throttle_time_ms: 200
      }

      assert {:ok, leave_group} = LeaveGroup.Response.parse_response(response)
      assert %LeaveGroupStruct{throttle_time_ms: 200} = leave_group
    end

    test "parses successful response with zero throttle time" do
      response = %Kayrock.LeaveGroup.V2.Response{
        error_code: 0,
        throttle_time_ms: 0
      }

      assert {:ok, leave_group} = LeaveGroup.Response.parse_response(response)
      assert leave_group.throttle_time_ms == 0
    end

    test "parses error response with unknown_member_id" do
      response = %Kayrock.LeaveGroup.V2.Response{
        error_code: 25,
        throttle_time_ms: 10
      }

      assert {:error, error} = LeaveGroup.Response.parse_response(response)
      assert error.error == :unknown_member_id
    end

    test "parses error response with rebalance_in_progress" do
      response = %Kayrock.LeaveGroup.V2.Response{
        error_code: 27,
        throttle_time_ms: 0
      }

      assert {:error, error} = LeaveGroup.Response.parse_response(response)
      assert error.error == :rebalance_in_progress
    end
  end

  # ---- V3 Response (batch leave with members) ----

  describe "V3 Response implementation (batch leave)" do
    test "parses successful response with members" do
      response = %Kayrock.LeaveGroup.V3.Response{
        error_code: 0,
        throttle_time_ms: 100,
        members: [
          %{member_id: "member-1", group_instance_id: "instance-1", error_code: 0},
          %{member_id: "member-2", group_instance_id: nil, error_code: 0}
        ]
      }

      assert {:ok, leave_group} = LeaveGroup.Response.parse_response(response)
      assert %LeaveGroupStruct{} = leave_group
      assert leave_group.throttle_time_ms == 100
      assert length(leave_group.members) == 2

      [first, second] = leave_group.members
      assert first.member_id == "member-1"
      assert first.group_instance_id == "instance-1"
      assert first.error == :no_error

      assert second.member_id == "member-2"
      assert second.group_instance_id == nil
      assert second.error == :no_error
    end

    test "parses successful response with per-member errors" do
      response = %Kayrock.LeaveGroup.V3.Response{
        error_code: 0,
        throttle_time_ms: 0,
        members: [
          %{member_id: "known", group_instance_id: nil, error_code: 0},
          %{member_id: "unknown", group_instance_id: nil, error_code: 25}
        ]
      }

      assert {:ok, leave_group} = LeaveGroup.Response.parse_response(response)

      [first, second] = leave_group.members
      assert first.error == :no_error
      assert second.error == :unknown_member_id
    end

    test "parses successful response with empty members list" do
      response = %Kayrock.LeaveGroup.V3.Response{
        error_code: 0,
        throttle_time_ms: 0,
        members: []
      }

      assert {:ok, leave_group} = LeaveGroup.Response.parse_response(response)
      assert leave_group.members == []
    end

    test "parses error response (top-level error)" do
      response = %Kayrock.LeaveGroup.V3.Response{
        error_code: 15,
        throttle_time_ms: 0,
        members: []
      }

      assert {:error, error} = LeaveGroup.Response.parse_response(response)
      assert error.error == :coordinator_not_available
    end

    test "parses error response ignoring members" do
      response = %Kayrock.LeaveGroup.V3.Response{
        error_code: 16,
        throttle_time_ms: 50,
        members: [
          %{member_id: "member-1", group_instance_id: nil, error_code: 0}
        ]
      }

      assert {:error, error} = LeaveGroup.Response.parse_response(response)
      assert error.error == :not_coordinator
      assert error.metadata == %{}
    end

    test "parses successful response with zero throttle time" do
      response = %Kayrock.LeaveGroup.V3.Response{
        error_code: 0,
        throttle_time_ms: 0,
        members: [
          %{member_id: "member-1", group_instance_id: "instance-1", error_code: 0}
        ]
      }

      assert {:ok, leave_group} = LeaveGroup.Response.parse_response(response)
      assert leave_group.throttle_time_ms == 0
    end
  end

  # ---- V4 Response (FLEX) ----

  describe "V4 Response implementation (FLEX)" do
    test "parses successful response (compact encoding handled by Kayrock)" do
      response = %Kayrock.LeaveGroup.V4.Response{
        error_code: 0,
        throttle_time_ms: 300,
        members: [
          %{
            member_id: "member-1",
            group_instance_id: "instance-1",
            error_code: 0,
            tagged_fields: []
          }
        ],
        tagged_fields: []
      }

      assert {:ok, leave_group} = LeaveGroup.Response.parse_response(response)
      assert %LeaveGroupStruct{throttle_time_ms: 300} = leave_group
      assert length(leave_group.members) == 1
      assert hd(leave_group.members).member_id == "member-1"
      assert hd(leave_group.members).error == :no_error
    end

    test "parses error response" do
      response = %Kayrock.LeaveGroup.V4.Response{
        error_code: 25,
        throttle_time_ms: 0,
        members: [],
        tagged_fields: []
      }

      assert {:error, error} = LeaveGroup.Response.parse_response(response)
      assert error.error == :unknown_member_id
    end

    test "preserves throttle_time_ms with tagged_fields" do
      response = %Kayrock.LeaveGroup.V4.Response{
        error_code: 0,
        throttle_time_ms: 500,
        members: [],
        tagged_fields: [{0, <<1, 2, 3>>}]
      }

      assert {:ok, leave_group} = LeaveGroup.Response.parse_response(response)
      assert leave_group.throttle_time_ms == 500
    end

    test "parses members with per-member errors" do
      response = %Kayrock.LeaveGroup.V4.Response{
        error_code: 0,
        throttle_time_ms: 0,
        members: [
          %{
            member_id: "ok-member",
            group_instance_id: "inst-1",
            error_code: 0,
            tagged_fields: []
          },
          %{
            member_id: "bad-member",
            group_instance_id: nil,
            error_code: 25,
            tagged_fields: []
          }
        ],
        tagged_fields: []
      }

      assert {:ok, leave_group} = LeaveGroup.Response.parse_response(response)
      assert length(leave_group.members) == 2

      [ok, bad] = leave_group.members
      assert ok.error == :no_error
      assert bad.error == :unknown_member_id
    end
  end

  # ---- Cross-version consistency ----

  describe "Version comparison" do
    test "V1 returns LeaveGroup struct while V0 returns atom" do
      v0_response = %Kayrock.LeaveGroup.V0.Response{error_code: 0}
      v1_response = %Kayrock.LeaveGroup.V1.Response{error_code: 0, throttle_time_ms: 50}

      v0_result = LeaveGroup.Response.parse_response(v0_response)
      v1_result = LeaveGroup.Response.parse_response(v1_response)

      assert v0_result == {:ok, :no_error}
      assert {:ok, %LeaveGroupStruct{throttle_time_ms: 50}} = v1_result
    end

    test "error responses are identical between V0-V2 versions" do
      v0_response = %Kayrock.LeaveGroup.V0.Response{error_code: 27}
      v1_response = %Kayrock.LeaveGroup.V1.Response{error_code: 27}
      v2_response = %Kayrock.LeaveGroup.V2.Response{error_code: 27}

      {:error, v0_error} = LeaveGroup.Response.parse_response(v0_response)
      {:error, v1_error} = LeaveGroup.Response.parse_response(v1_response)
      {:error, v2_error} = LeaveGroup.Response.parse_response(v2_response)

      for error <- [v0_error, v1_error, v2_error] do
        assert error.error == :rebalance_in_progress
        assert error.metadata == %{}
      end
    end
  end

  describe "Cross-version response consistency (V1-V2)" do
    @v1_v2_response_structs [
      {struct(Kayrock.LeaveGroup.V1.Response, %{
         throttle_time_ms: 100,
         error_code: 0
       }), "V1"},
      {struct(Kayrock.LeaveGroup.V2.Response, %{
         throttle_time_ms: 100,
         error_code: 0
       }), "V2"}
    ]

    test "V1-V2 produce identical domain results" do
      results =
        Enum.map(@v1_v2_response_structs, fn {response, label} ->
          {:ok, result} = LeaveGroup.Response.parse_response(response)
          {label, result}
        end)

      for {label, result} <- results do
        assert %LeaveGroupStruct{} = result,
               "#{label}: expected LeaveGroup struct"

        assert result.throttle_time_ms == 100,
               "#{label}: expected throttle_time_ms 100, got #{result.throttle_time_ms}"

        assert result.members == nil,
               "#{label}: expected members nil for V1/V2"
      end
    end
  end

  describe "Cross-version response consistency (V3-V4)" do
    test "V3 and V4 produce identical domain results for same data" do
      members = [
        %{member_id: "m1", group_instance_id: "i1", error_code: 0},
        %{member_id: "m2", group_instance_id: nil, error_code: 25}
      ]

      v3_response =
        struct(Kayrock.LeaveGroup.V3.Response, %{
          throttle_time_ms: 200,
          error_code: 0,
          members: members
        })

      v4_response =
        struct(Kayrock.LeaveGroup.V4.Response, %{
          throttle_time_ms: 200,
          error_code: 0,
          members: members,
          tagged_fields: []
        })

      {:ok, v3_result} = LeaveGroup.Response.parse_response(v3_response)
      {:ok, v4_result} = LeaveGroup.Response.parse_response(v4_response)

      assert v3_result.throttle_time_ms == v4_result.throttle_time_ms
      assert v3_result.members == v4_result.members
      assert length(v3_result.members) == 2

      [first, second] = v3_result.members
      assert first.error == :no_error
      assert second.error == :unknown_member_id
    end

    test "V0 returns atom, V1/V2 return struct without members, V3/V4 return struct with members" do
      v0_response = %Kayrock.LeaveGroup.V0.Response{error_code: 0}

      v1_response = %Kayrock.LeaveGroup.V1.Response{
        error_code: 0,
        throttle_time_ms: 0
      }

      v3_response = %Kayrock.LeaveGroup.V3.Response{
        error_code: 0,
        throttle_time_ms: 0,
        members: [
          %{member_id: "m1", group_instance_id: nil, error_code: 0}
        ]
      }

      {:ok, v0_result} = LeaveGroup.Response.parse_response(v0_response)
      {:ok, v1_result} = LeaveGroup.Response.parse_response(v1_response)
      {:ok, v3_result} = LeaveGroup.Response.parse_response(v3_response)

      assert v0_result == :no_error
      assert %LeaveGroupStruct{members: nil} = v1_result
      assert %LeaveGroupStruct{members: [_ | _]} = v3_result
    end
  end

  describe "Error responses across all versions" do
    test "all versions produce identical error for same error code" do
      error_code = 27

      responses = [
        %Kayrock.LeaveGroup.V0.Response{error_code: error_code},
        %Kayrock.LeaveGroup.V1.Response{error_code: error_code, throttle_time_ms: 0},
        %Kayrock.LeaveGroup.V2.Response{error_code: error_code, throttle_time_ms: 0},
        %Kayrock.LeaveGroup.V3.Response{
          error_code: error_code,
          throttle_time_ms: 0,
          members: []
        },
        %Kayrock.LeaveGroup.V4.Response{
          error_code: error_code,
          throttle_time_ms: 0,
          members: [],
          tagged_fields: []
        }
      ]

      errors =
        Enum.map(responses, fn resp ->
          {:error, error} = LeaveGroup.Response.parse_response(resp)
          error
        end)

      for error <- errors do
        assert error.error == :rebalance_in_progress
        assert error.metadata == %{}
      end
    end
  end

  # ---- Any fallback ----

  describe "Any fallback Response implementation" do
    test "routes V3+-like map to members path" do
      response = %{
        throttle_time_ms: 100,
        error_code: 0,
        members: [
          %{member_id: "m1", group_instance_id: nil, error_code: 0}
        ]
      }

      assert {:ok, leave_group} = LeaveGroup.Response.parse_response(response)
      assert %LeaveGroupStruct{} = leave_group
      assert leave_group.throttle_time_ms == 100
      assert length(leave_group.members) == 1
      assert hd(leave_group.members).member_id == "m1"
      assert hd(leave_group.members).error == :no_error
    end

    test "routes V1/V2-like map to throttle_time path" do
      response = %{
        throttle_time_ms: 250,
        error_code: 0
      }

      assert {:ok, leave_group} = LeaveGroup.Response.parse_response(response)
      assert %LeaveGroupStruct{throttle_time_ms: 250} = leave_group
      assert leave_group.members == nil
    end

    test "routes V0-like map to :no_error path" do
      response = %{
        error_code: 0
      }

      assert {:ok, :no_error} = LeaveGroup.Response.parse_response(response)
    end

    test "handles error response via V3+ Any path" do
      response = %{
        throttle_time_ms: 0,
        error_code: 16,
        members: []
      }

      assert {:error, error} = LeaveGroup.Response.parse_response(response)
      assert error.error == :not_coordinator
    end

    test "handles error response via V1/V2 Any path" do
      response = %{
        throttle_time_ms: 0,
        error_code: 22
      }

      assert {:error, error} = LeaveGroup.Response.parse_response(response)
      assert error.error == :illegal_generation
    end

    test "handles error response via V0 Any path" do
      response = %{
        error_code: 27
      }

      assert {:error, error} = LeaveGroup.Response.parse_response(response)
      assert error.error == :rebalance_in_progress
    end

    test "V0 path returns :no_error atom (not LeaveGroup struct)" do
      response = %{error_code: 0}

      result = LeaveGroup.Response.parse_response(response)

      assert result == {:ok, :no_error}
      refute match?({:ok, %LeaveGroupStruct{}}, result)
    end

    test "V3+ path with per-member errors via Any path" do
      response = %{
        throttle_time_ms: 50,
        error_code: 0,
        members: [
          %{member_id: "ok", group_instance_id: nil, error_code: 0},
          %{member_id: "bad", group_instance_id: "inst-x", error_code: 25}
        ]
      }

      assert {:ok, leave_group} = LeaveGroup.Response.parse_response(response)
      assert length(leave_group.members) == 2

      [ok, bad] = leave_group.members
      assert ok.error == :no_error
      assert bad.error == :unknown_member_id
      assert bad.group_instance_id == "inst-x"
    end

    test "handles zero throttle_time_ms via V1/V2 Any path" do
      response = %{
        throttle_time_ms: 0,
        error_code: 0
      }

      assert {:ok, leave_group} = LeaveGroup.Response.parse_response(response)
      assert leave_group.throttle_time_ms == 0
    end
  end
end
