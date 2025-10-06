defmodule KafkaEx.New.Protocols.Kayrock.LeaveGroup.V0ResponseImplTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Protocols.Kayrock.LeaveGroup

  describe "parse_response/1 for V0" do
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
end
