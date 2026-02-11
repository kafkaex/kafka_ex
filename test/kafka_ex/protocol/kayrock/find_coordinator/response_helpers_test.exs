defmodule KafkaEx.Protocol.Kayrock.FindCoordinator.ResponseHelpersTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Protocol.Kayrock.FindCoordinator.ResponseHelpers
  alias KafkaEx.Cluster.Broker
  alias KafkaEx.Client.Error
  alias KafkaEx.Messages.FindCoordinator

  describe "parse_coordinator_from_response/1" do
    test "returns nil for nil node_id" do
      assert ResponseHelpers.parse_coordinator_from_response(%{node_id: nil}) == nil
    end

    test "parses response with coordinator fields to Broker struct" do
      response = %{node_id: 1, host: "broker1.example.com", port: 9092}

      result = ResponseHelpers.parse_coordinator_from_response(response)

      assert %Broker{} = result
      assert result.node_id == 1
      assert result.host == "broker1.example.com"
      assert result.port == 9092
    end
  end

  describe "check_error/2" do
    test "returns ok with fields when no error" do
      fields = [coordinator: %Broker{node_id: 1, host: "host", port: 9092}]

      assert {:ok, result_fields} = ResponseHelpers.check_error(0, fields)
      assert Keyword.get(result_fields, :error_code) == :no_error
      assert Keyword.get(result_fields, :coordinator) == %Broker{node_id: 1, host: "host", port: 9092}
    end

    test "returns error for non-zero error code" do
      fields = []

      assert {:error, %Error{}} = ResponseHelpers.check_error(15, fields)
    end
  end

  describe "parse_v0_response/1" do
    test "parses successful V0 response" do
      response = %{
        error_code: 0,
        node_id: 1,
        host: "broker1",
        port: 9092
      }

      assert {:ok, %FindCoordinator{} = result} = ResponseHelpers.parse_v0_response(response)
      assert result.error_code == :no_error
      assert result.coordinator.node_id == 1
      assert result.coordinator.host == "broker1"
      assert result.coordinator.port == 9092
    end

    test "returns error for V0 response with error code" do
      response = %{
        error_code: 15,
        node_id: nil,
        host: nil,
        port: nil
      }

      assert {:error, %Error{}} = ResponseHelpers.parse_v0_response(response)
    end
  end

  describe "parse_v1_response/1" do
    test "parses successful V1 response with throttle_time_ms" do
      response = %{
        error_code: 0,
        error_message: nil,
        throttle_time_ms: 100,
        node_id: 2,
        host: "broker2",
        port: 9093
      }

      assert {:ok, %FindCoordinator{} = result} = ResponseHelpers.parse_v1_response(response)
      assert result.error_code == :no_error
      assert result.throttle_time_ms == 100
      assert result.coordinator.node_id == 2
    end

    test "parses V1 response with error_message" do
      response = %{
        error_code: 0,
        error_message: "some message",
        throttle_time_ms: 0,
        node_id: 1,
        host: "broker1",
        port: 9092
      }

      assert {:ok, %FindCoordinator{} = result} = ResponseHelpers.parse_v1_response(response)
      assert result.error_message == "some message"
    end

    test "returns error for V1 response with error code" do
      response = %{
        error_code: 15,
        error_message: "coordinator not available",
        throttle_time_ms: 0,
        node_id: nil,
        host: nil,
        port: nil
      }

      assert {:error, %Error{}} = ResponseHelpers.parse_v1_response(response)
    end
  end
end
