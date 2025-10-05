defmodule KafkaEx.New.Protocols.Kayrock.Heartbeat.V1ResponseImplTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Protocols.Kayrock.Heartbeat
  alias KafkaEx.New.Structs.Heartbeat, as: HeartbeatStruct

  describe "parse_response/1 for V1" do
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

    test "v1 returns Heartbeat struct while v0 returns atom" do
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
