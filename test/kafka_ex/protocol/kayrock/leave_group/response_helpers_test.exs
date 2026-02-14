defmodule KafkaEx.Protocol.Kayrock.LeaveGroup.ResponseHelpersTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Protocol.Kayrock.LeaveGroup.ResponseHelpers
  alias KafkaEx.Messages.LeaveGroup
  alias KafkaEx.Client.Error

  # ---- parse_v0_response/1 ----

  describe "parse_v0_response/1" do
    test "returns {:ok, :no_error} on success" do
      response = %{error_code: 0}

      assert {:ok, :no_error} = ResponseHelpers.parse_v0_response(response)
    end

    test "returns error for rebalance_in_progress" do
      response = %{error_code: 27}

      assert {:error, %Error{} = error} = ResponseHelpers.parse_v0_response(response)
      assert error.error == :rebalance_in_progress
      assert error.metadata == %{}
    end

    test "returns error for unknown_member_id" do
      response = %{error_code: 25}

      assert {:error, %Error{} = error} = ResponseHelpers.parse_v0_response(response)
      assert error.error == :unknown_member_id
    end

    test "returns error for coordinator_not_available" do
      response = %{error_code: 15}

      assert {:error, %Error{} = error} = ResponseHelpers.parse_v0_response(response)
      assert error.error == :coordinator_not_available
    end

    test "returns error for not_coordinator" do
      response = %{error_code: 16}

      assert {:error, %Error{} = error} = ResponseHelpers.parse_v0_response(response)
      assert error.error == :not_coordinator
    end

    test "returns error for group_authorization_failed" do
      response = %{error_code: 30}

      assert {:error, %Error{} = error} = ResponseHelpers.parse_v0_response(response)
      assert error.error == :group_authorization_failed
    end

    test "error struct always has empty metadata" do
      response = %{error_code: 27}

      {:error, error} = ResponseHelpers.parse_v0_response(response)

      assert error.metadata == %{}
    end

    test "handles various error codes data-driven" do
      error_codes = [
        {15, :coordinator_not_available},
        {16, :not_coordinator},
        {25, :unknown_member_id},
        {27, :rebalance_in_progress},
        {30, :group_authorization_failed},
        {69, :group_id_not_found}
      ]

      for {code, expected_atom} <- error_codes do
        response = %{error_code: code}

        assert {:error, %Error{} = error} = ResponseHelpers.parse_v0_response(response),
               "expected error for code #{code}"

        assert error.error == expected_atom,
               "expected #{expected_atom} for code #{code}, got #{error.error}"
      end
    end
  end

  # ---- parse_v1_v2_response/1 ----

  describe "parse_v1_v2_response/1" do
    test "returns {:ok, %LeaveGroup{}} on success with throttle_time_ms" do
      response = %{error_code: 0, throttle_time_ms: 100}

      assert {:ok, %LeaveGroup{} = leave_group} = ResponseHelpers.parse_v1_v2_response(response)
      assert leave_group.throttle_time_ms == 100
      assert leave_group.members == nil
    end

    test "returns success with zero throttle_time_ms" do
      response = %{error_code: 0, throttle_time_ms: 0}

      assert {:ok, %LeaveGroup{} = leave_group} = ResponseHelpers.parse_v1_v2_response(response)
      assert leave_group.throttle_time_ms == 0
    end

    test "returns success with large throttle_time_ms" do
      response = %{error_code: 0, throttle_time_ms: 60_000}

      assert {:ok, %LeaveGroup{} = leave_group} = ResponseHelpers.parse_v1_v2_response(response)
      assert leave_group.throttle_time_ms == 60_000
    end

    test "returns error for rebalance_in_progress" do
      response = %{error_code: 27, throttle_time_ms: 0}

      assert {:error, %Error{} = error} = ResponseHelpers.parse_v1_v2_response(response)
      assert error.error == :rebalance_in_progress
      assert error.metadata == %{}
    end

    test "returns error for unknown_member_id" do
      response = %{error_code: 25, throttle_time_ms: 50}

      assert {:error, %Error{} = error} = ResponseHelpers.parse_v1_v2_response(response)
      assert error.error == :unknown_member_id
    end

    test "error struct always has empty metadata" do
      response = %{error_code: 25, throttle_time_ms: 100}

      {:error, error} = ResponseHelpers.parse_v1_v2_response(response)

      assert error.metadata == %{}
    end

    test "handles various error codes data-driven" do
      error_codes = [
        {15, :coordinator_not_available},
        {16, :not_coordinator},
        {25, :unknown_member_id},
        {27, :rebalance_in_progress},
        {30, :group_authorization_failed},
        {69, :group_id_not_found}
      ]

      for {code, expected_atom} <- error_codes do
        response = %{error_code: code, throttle_time_ms: 0}

        assert {:error, %Error{} = error} = ResponseHelpers.parse_v1_v2_response(response),
               "expected error for code #{code}"

        assert error.error == expected_atom,
               "expected #{expected_atom} for code #{code}, got #{error.error}"
      end
    end
  end

  # ---- parse_v3_plus_response/1 ----

  describe "parse_v3_plus_response/1" do
    test "returns {:ok, %LeaveGroup{}} on success with members" do
      response = %{
        error_code: 0,
        throttle_time_ms: 100,
        members: [
          %{member_id: "member-1", group_instance_id: "instance-1", error_code: 0}
        ]
      }

      assert {:ok, %LeaveGroup{} = leave_group} =
               ResponseHelpers.parse_v3_plus_response(response)

      assert leave_group.throttle_time_ms == 100
      assert length(leave_group.members) == 1

      assert hd(leave_group.members) == %{
               member_id: "member-1",
               group_instance_id: "instance-1",
               error: :no_error
             }
    end

    test "returns success with empty members list" do
      response = %{
        error_code: 0,
        throttle_time_ms: 0,
        members: []
      }

      assert {:ok, %LeaveGroup{} = leave_group} =
               ResponseHelpers.parse_v3_plus_response(response)

      assert leave_group.throttle_time_ms == 0
      assert leave_group.members == []
    end

    test "returns success with multiple members (all no_error)" do
      response = %{
        error_code: 0,
        throttle_time_ms: 50,
        members: [
          %{member_id: "member-1", group_instance_id: "instance-1", error_code: 0},
          %{member_id: "member-2", group_instance_id: nil, error_code: 0},
          %{member_id: "member-3", group_instance_id: "instance-3", error_code: 0}
        ]
      }

      assert {:ok, %LeaveGroup{} = leave_group} =
               ResponseHelpers.parse_v3_plus_response(response)

      assert length(leave_group.members) == 3

      for member <- leave_group.members do
        assert member.error == :no_error
      end

      assert Enum.at(leave_group.members, 1).group_instance_id == nil
    end

    test "returns success with per-member errors" do
      response = %{
        error_code: 0,
        throttle_time_ms: 0,
        members: [
          %{member_id: "known-member", group_instance_id: "instance-1", error_code: 0},
          %{member_id: "unknown-member", group_instance_id: nil, error_code: 25}
        ]
      }

      assert {:ok, %LeaveGroup{} = leave_group} =
               ResponseHelpers.parse_v3_plus_response(response)

      [first, second] = leave_group.members

      assert first.member_id == "known-member"
      assert first.error == :no_error

      assert second.member_id == "unknown-member"
      assert second.error == :unknown_member_id
    end

    test "returns success when all members have errors (top-level is no_error)" do
      response = %{
        error_code: 0,
        throttle_time_ms: 0,
        members: [
          %{member_id: "member-1", group_instance_id: nil, error_code: 25},
          %{member_id: "member-2", group_instance_id: nil, error_code: 25}
        ]
      }

      # Top-level no_error means the request was processed, but individual
      # members may have errors. This is still an {:ok, ...} response.
      assert {:ok, %LeaveGroup{} = leave_group} =
               ResponseHelpers.parse_v3_plus_response(response)

      for member <- leave_group.members do
        assert member.error == :unknown_member_id
      end
    end

    test "returns error when top-level error_code is non-zero" do
      response = %{
        error_code: 15,
        throttle_time_ms: 0,
        members: []
      }

      assert {:error, %Error{} = error} = ResponseHelpers.parse_v3_plus_response(response)
      assert error.error == :coordinator_not_available
    end

    test "returns error ignoring members when top-level error" do
      response = %{
        error_code: 16,
        throttle_time_ms: 100,
        members: [
          %{member_id: "member-1", group_instance_id: nil, error_code: 0}
        ]
      }

      assert {:error, %Error{} = error} = ResponseHelpers.parse_v3_plus_response(response)
      assert error.error == :not_coordinator
      assert error.metadata == %{}
    end

    test "handles member with nil group_instance_id" do
      response = %{
        error_code: 0,
        throttle_time_ms: 0,
        members: [
          %{member_id: "dynamic-member", group_instance_id: nil, error_code: 0}
        ]
      }

      assert {:ok, %LeaveGroup{} = leave_group} =
               ResponseHelpers.parse_v3_plus_response(response)

      assert hd(leave_group.members).group_instance_id == nil
    end

    test "maps various per-member error codes data-driven" do
      member_error_codes = [
        {0, :no_error},
        {25, :unknown_member_id},
        {79, :member_id_required}
      ]

      for {code, expected_atom} <- member_error_codes do
        response = %{
          error_code: 0,
          throttle_time_ms: 0,
          members: [
            %{member_id: "test-member", group_instance_id: nil, error_code: code}
          ]
        }

        assert {:ok, %LeaveGroup{} = leave_group} =
                 ResponseHelpers.parse_v3_plus_response(response),
               "expected success for member error code #{code}"

        assert hd(leave_group.members).error == expected_atom,
               "expected #{expected_atom} for member error code #{code}, got #{hd(leave_group.members).error}"
      end
    end

    test "handles various top-level error codes data-driven" do
      error_codes = [
        {15, :coordinator_not_available},
        {16, :not_coordinator},
        {25, :unknown_member_id},
        {27, :rebalance_in_progress},
        {30, :group_authorization_failed},
        {69, :group_id_not_found}
      ]

      for {code, expected_atom} <- error_codes do
        response = %{error_code: code, throttle_time_ms: 0, members: []}

        assert {:error, %Error{} = error} = ResponseHelpers.parse_v3_plus_response(response),
               "expected error for code #{code}"

        assert error.error == expected_atom,
               "expected #{expected_atom} for code #{code}, got #{error.error}"
      end
    end
  end
end
