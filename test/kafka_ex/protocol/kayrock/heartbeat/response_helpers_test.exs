defmodule KafkaEx.Protocol.Kayrock.Heartbeat.ResponseHelpersTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Protocol.Kayrock.Heartbeat.ResponseHelpers
  alias KafkaEx.Messages.Heartbeat
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

    test "returns error for illegal_generation" do
      response = %{error_code: 22}

      assert {:error, %Error{} = error} = ResponseHelpers.parse_v0_response(response)
      assert error.error == :illegal_generation
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
        {22, :illegal_generation},
        {25, :unknown_member_id},
        {27, :rebalance_in_progress},
        {30, :group_authorization_failed}
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

  # ---- parse_v1_plus_response/1 ----

  describe "parse_v1_plus_response/1" do
    test "returns {:ok, %Heartbeat{}} on success with throttle_time_ms" do
      response = %{error_code: 0, throttle_time_ms: 100}

      assert {:ok, %Heartbeat{} = heartbeat} = ResponseHelpers.parse_v1_plus_response(response)
      assert heartbeat.throttle_time_ms == 100
    end

    test "returns success with zero throttle_time_ms" do
      response = %{error_code: 0, throttle_time_ms: 0}

      assert {:ok, %Heartbeat{} = heartbeat} = ResponseHelpers.parse_v1_plus_response(response)
      assert heartbeat.throttle_time_ms == 0
    end

    test "returns success with large throttle_time_ms" do
      response = %{error_code: 0, throttle_time_ms: 60_000}

      assert {:ok, %Heartbeat{} = heartbeat} = ResponseHelpers.parse_v1_plus_response(response)
      assert heartbeat.throttle_time_ms == 60_000
    end

    test "returns error for rebalance_in_progress" do
      response = %{error_code: 27, throttle_time_ms: 0}

      assert {:error, %Error{} = error} = ResponseHelpers.parse_v1_plus_response(response)
      assert error.error == :rebalance_in_progress
      assert error.metadata == %{}
    end

    test "returns error for unknown_member_id" do
      response = %{error_code: 25, throttle_time_ms: 50}

      assert {:error, %Error{} = error} = ResponseHelpers.parse_v1_plus_response(response)
      assert error.error == :unknown_member_id
    end

    test "returns error for illegal_generation" do
      response = %{error_code: 22, throttle_time_ms: 10}

      assert {:error, %Error{} = error} = ResponseHelpers.parse_v1_plus_response(response)
      assert error.error == :illegal_generation
    end

    test "error struct always has empty metadata" do
      response = %{error_code: 25, throttle_time_ms: 100}

      {:error, error} = ResponseHelpers.parse_v1_plus_response(response)

      assert error.metadata == %{}
    end

    test "handles various error codes data-driven" do
      error_codes = [
        {15, :coordinator_not_available},
        {16, :not_coordinator},
        {22, :illegal_generation},
        {25, :unknown_member_id},
        {27, :rebalance_in_progress},
        {30, :group_authorization_failed}
      ]

      for {code, expected_atom} <- error_codes do
        response = %{error_code: code, throttle_time_ms: 0}

        assert {:error, %Error{} = error} = ResponseHelpers.parse_v1_plus_response(response),
               "expected error for code #{code}"

        assert error.error == expected_atom,
               "expected #{expected_atom} for code #{code}, got #{error.error}"
      end
    end
  end
end
