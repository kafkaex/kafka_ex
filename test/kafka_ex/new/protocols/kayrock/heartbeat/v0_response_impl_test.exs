defmodule KafkaEx.New.Protocols.Kayrock.Heartbeat.V0ResponseImplTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Protocols.Kayrock.Heartbeat

  describe "parse_response/1 for V0" do
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
end
