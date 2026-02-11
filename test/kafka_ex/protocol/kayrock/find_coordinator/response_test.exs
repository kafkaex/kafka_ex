defmodule KafkaEx.Protocol.Kayrock.FindCoordinator.ResponseTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Protocol.Kayrock.FindCoordinator.Response
  alias KafkaEx.Messages.FindCoordinator

  describe "V0 Response implementation" do
    test "parses successful response" do
      response = %Kayrock.FindCoordinator.V0.Response{
        error_code: 0,
        node_id: 1,
        host: "broker1.example.com",
        port: 9092
      }

      assert {:ok, %FindCoordinator{} = result} = Response.parse_response(response)

      assert result.error_code == :no_error
      assert result.coordinator.node_id == 1
      assert result.coordinator.host == "broker1.example.com"
      assert result.coordinator.port == 9092
      # V0 has no throttle_time_ms or error_message
      assert is_nil(result.throttle_time_ms)
      assert is_nil(result.error_message)
    end

    test "parses response with error" do
      response = %Kayrock.FindCoordinator.V0.Response{
        error_code: 15,
        node_id: nil,
        host: nil,
        port: nil
      }

      assert {:error, error} = Response.parse_response(response)
      assert error.error == :coordinator_not_available
    end

    test "parses response with group_coordinator_not_available error" do
      response = %Kayrock.FindCoordinator.V0.Response{
        error_code: 15,
        node_id: nil,
        host: nil,
        port: nil
      }

      assert {:error, error} = Response.parse_response(response)
      assert error.error == :coordinator_not_available
    end

    test "parses response with not_coordinator error" do
      response = %Kayrock.FindCoordinator.V0.Response{
        error_code: 16,
        node_id: nil,
        host: nil,
        port: nil
      }

      assert {:error, error} = Response.parse_response(response)
      assert error.error == :not_coordinator
    end
  end

  describe "V1 Response implementation" do
    test "parses successful response with all fields" do
      response = %Kayrock.FindCoordinator.V1.Response{
        throttle_time_ms: 50,
        error_code: 0,
        error_message: nil,
        node_id: 2,
        host: "broker2.example.com",
        port: 9093
      }

      assert {:ok, %FindCoordinator{} = result} = Response.parse_response(response)

      assert result.error_code == :no_error
      assert result.throttle_time_ms == 50
      assert is_nil(result.error_message)
      assert result.coordinator.node_id == 2
      assert result.coordinator.host == "broker2.example.com"
      assert result.coordinator.port == 9093
    end

    test "parses response with error and error_message" do
      response = %Kayrock.FindCoordinator.V1.Response{
        throttle_time_ms: 0,
        error_code: 15,
        error_message: "Coordinator not available",
        node_id: nil,
        host: nil,
        port: nil
      }

      assert {:error, error} = Response.parse_response(response)
      assert error.error == :coordinator_not_available
    end

    test "parses response with throttling" do
      response = %Kayrock.FindCoordinator.V1.Response{
        throttle_time_ms: 1000,
        error_code: 0,
        error_message: nil,
        node_id: 3,
        host: "broker3.example.com",
        port: 9094
      }

      assert {:ok, result} = Response.parse_response(response)

      assert result.throttle_time_ms == 1000
    end

    test "parses response for transaction coordinator" do
      # The response format is the same regardless of coordinator type
      response = %Kayrock.FindCoordinator.V1.Response{
        throttle_time_ms: 0,
        error_code: 0,
        error_message: nil,
        node_id: 5,
        host: "tx-broker.example.com",
        port: 9092
      }

      assert {:ok, result} = Response.parse_response(response)

      assert result.coordinator.node_id == 5
      assert result.coordinator.host == "tx-broker.example.com"
    end
  end

  describe "Error handling" do
    test "returns error for coordinator_not_available" do
      response = %Kayrock.FindCoordinator.V0.Response{
        error_code: 15,
        node_id: nil,
        host: nil,
        port: nil
      }

      assert {:error, error} = Response.parse_response(response)
      assert error.error == :coordinator_not_available
    end

    test "returns error for not_coordinator" do
      response = %Kayrock.FindCoordinator.V1.Response{
        throttle_time_ms: 0,
        error_code: 16,
        error_message: "This is not the coordinator",
        node_id: nil,
        host: nil,
        port: nil
      }

      assert {:error, error} = Response.parse_response(response)
      assert error.error == :not_coordinator
    end

    test "returns error for group_authorization_failed" do
      response = %Kayrock.FindCoordinator.V1.Response{
        throttle_time_ms: 0,
        error_code: 30,
        error_message: "Authorization failed",
        node_id: nil,
        host: nil,
        port: nil
      }

      assert {:error, error} = Response.parse_response(response)
      assert error.error == :group_authorization_failed
    end

    test "returns error for invalid_group_id" do
      response = %Kayrock.FindCoordinator.V1.Response{
        throttle_time_ms: 0,
        error_code: 24,
        error_message: "Invalid group ID",
        node_id: nil,
        host: nil,
        port: nil
      }

      assert {:error, error} = Response.parse_response(response)
      assert error.error == :invalid_group_id
    end
  end

  describe "Version comparison" do
    test "V0 response has no throttle_time_ms" do
      v0_response = %Kayrock.FindCoordinator.V0.Response{
        error_code: 0,
        node_id: 1,
        host: "localhost",
        port: 9092
      }

      assert {:ok, result} = Response.parse_response(v0_response)
      assert is_nil(result.throttle_time_ms)
      assert is_nil(result.error_message)
    end

    test "V1 response has throttle_time_ms and error_message" do
      v1_response = %Kayrock.FindCoordinator.V1.Response{
        throttle_time_ms: 100,
        error_code: 0,
        error_message: nil,
        node_id: 1,
        host: "localhost",
        port: 9092
      }

      assert {:ok, result} = Response.parse_response(v1_response)
      assert result.throttle_time_ms == 100
    end
  end

  describe "Coordinator parsing" do
    test "handles nil coordinator" do
      # When there's an error, coordinator may be nil
      response = %Kayrock.FindCoordinator.V0.Response{
        error_code: 0,
        node_id: nil,
        host: nil,
        port: nil
      }

      assert {:ok, result} = Response.parse_response(response)
      assert is_nil(result.coordinator)
    end

    test "parses coordinator with all fields" do
      response = %Kayrock.FindCoordinator.V1.Response{
        throttle_time_ms: 0,
        error_code: 0,
        error_message: nil,
        node_id: 42,
        host: "kafka-broker-42.internal",
        port: 19092
      }

      assert {:ok, result} = Response.parse_response(response)

      assert result.coordinator.node_id == 42
      assert result.coordinator.host == "kafka-broker-42.internal"
      assert result.coordinator.port == 19092
    end
  end
end
