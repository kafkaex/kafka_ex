defmodule KafkaEx.New.Protocols.Kayrock.LeaveGroup.V1ResponseImplTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Protocols.Kayrock.LeaveGroup
  alias KafkaEx.New.Structs.LeaveGroup, as: LeaveGroupStruct

  describe "parse_response/1 for V1" do
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

    test "v1 returns LeaveGroup struct while v0 returns atom" do
      v0_response = %Kayrock.LeaveGroup.V0.Response{error_code: 0}
      v1_response = %Kayrock.LeaveGroup.V1.Response{error_code: 0, throttle_time_ms: 50}

      v0_result = LeaveGroup.Response.parse_response(v0_response)
      v1_result = LeaveGroup.Response.parse_response(v1_response)

      assert v0_result == {:ok, :no_error}
      assert {:ok, %LeaveGroupStruct{throttle_time_ms: 50}} = v1_result
    end

    test "error responses are identical between versions" do
      v0_response = %Kayrock.LeaveGroup.V0.Response{error_code: 27}
      v1_response = %Kayrock.LeaveGroup.V1.Response{error_code: 27}

      {:error, v0_error} = LeaveGroup.Response.parse_response(v0_response)
      {:error, v1_error} = LeaveGroup.Response.parse_response(v1_response)

      assert v0_error.error == v1_error.error
      assert v0_error.metadata == v1_error.metadata
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
end
