defmodule KafkaEx.New.Protocols.Kayrock.Heartbeat.ResponseTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Protocols.Kayrock.Heartbeat
  alias KafkaEx.New.Structs.Heartbeat, as: HeartbeatStruct

  describe "V0 Response implementation" do
    test "parses successful response with no error" do
      response = %Kayrock.Heartbeat.V0.Response{
        error_code: 0
      }

      assert {:ok, :no_error} = Heartbeat.Response.parse_response(response)
    end

    test "parses error response with rebalance_in_progress" do
      response = %Kayrock.Heartbeat.V0.Response{
        error_code: 27
      }

      assert {:error, error} = Heartbeat.Response.parse_response(response)
      assert error.error == :rebalance_in_progress
      assert error.metadata == %{}
    end

    test "parses error response with unknown_member_id" do
      response = %Kayrock.Heartbeat.V0.Response{
        error_code: 25
      }

      assert {:error, error} = Heartbeat.Response.parse_response(response)
      assert error.error == :unknown_member_id
    end

    test "parses error response with illegal_generation" do
      response = %Kayrock.Heartbeat.V0.Response{
        error_code: 22
      }

      assert {:error, error} = Heartbeat.Response.parse_response(response)
      assert error.error == :illegal_generation
    end

    test "parses error response with coordinator_not_available" do
      response = %Kayrock.Heartbeat.V0.Response{
        error_code: 15
      }

      assert {:error, error} = Heartbeat.Response.parse_response(response)
      assert error.error == :coordinator_not_available
    end

    test "parses error response with not_coordinator" do
      response = %Kayrock.Heartbeat.V0.Response{
        error_code: 16
      }

      assert {:error, error} = Heartbeat.Response.parse_response(response)
      assert error.error == :not_coordinator
    end

    test "parses error response with group_authorization_failed" do
      response = %Kayrock.Heartbeat.V0.Response{
        error_code: 30
      }

      assert {:error, error} = Heartbeat.Response.parse_response(response)
      assert error.error == :group_authorization_failed
    end

    test "error struct has empty metadata" do
      response = %Kayrock.Heartbeat.V0.Response{
        error_code: 27
      }

      {:error, error} = Heartbeat.Response.parse_response(response)

      assert error.metadata == %{}
    end
  end

  describe "V1 Response implementation" do
    test "parses successful response with throttle_time_ms" do
      response = %Kayrock.Heartbeat.V1.Response{
        error_code: 0,
        throttle_time_ms: 0
      }

      assert {:ok, heartbeat} = Heartbeat.Response.parse_response(response)
      assert %HeartbeatStruct{throttle_time_ms: 0} = heartbeat
    end

    test "parses successful response with non-zero throttle time" do
      response = %Kayrock.Heartbeat.V1.Response{
        error_code: 0,
        throttle_time_ms: 100
      }

      assert {:ok, heartbeat} = Heartbeat.Response.parse_response(response)
      assert heartbeat.throttle_time_ms == 100
    end

    test "parses error response with rebalance_in_progress" do
      response = %Kayrock.Heartbeat.V1.Response{
        error_code: 27
      }

      assert {:error, error} = Heartbeat.Response.parse_response(response)
      assert error.error == :rebalance_in_progress
    end

    test "parses error response with unknown_member_id" do
      response = %Kayrock.Heartbeat.V1.Response{
        error_code: 25
      }

      assert {:error, error} = Heartbeat.Response.parse_response(response)
      assert error.error == :unknown_member_id
    end

    test "parses error response with illegal_generation" do
      response = %Kayrock.Heartbeat.V1.Response{
        error_code: 22
      }

      assert {:error, error} = Heartbeat.Response.parse_response(response)
      assert error.error == :illegal_generation
    end
  end

  describe "Version comparison" do
    test "V1 returns Heartbeat struct while V0 returns atom" do
      v0_response = %Kayrock.Heartbeat.V0.Response{error_code: 0}
      v1_response = %Kayrock.Heartbeat.V1.Response{error_code: 0, throttle_time_ms: 50}

      v0_result = Heartbeat.Response.parse_response(v0_response)
      v1_result = Heartbeat.Response.parse_response(v1_response)

      assert v0_result == {:ok, :no_error}
      assert {:ok, %HeartbeatStruct{throttle_time_ms: 50}} = v1_result
    end

    test "error responses are identical between versions" do
      v0_response = %Kayrock.Heartbeat.V0.Response{error_code: 27}
      v1_response = %Kayrock.Heartbeat.V1.Response{error_code: 27}

      {:error, v0_error} = Heartbeat.Response.parse_response(v0_response)
      {:error, v1_error} = Heartbeat.Response.parse_response(v1_response)

      assert v0_error.error == v1_error.error
      assert v0_error.metadata == v1_error.metadata
    end
  end
end
