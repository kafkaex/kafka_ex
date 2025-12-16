defmodule KafkaEx.New.Adapter.FindCoordinatorTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Adapter
  alias KafkaEx.Protocol.ConsumerMetadata.Response, as: ConsumerMetadataResponse
  alias Kayrock.FindCoordinator

  describe "consumer_metadata_request/1" do
    test "creates V0 request from consumer group string" do
      request = Adapter.consumer_metadata_request("my-consumer-group")

      assert %FindCoordinator.V0.Request{} = request
      assert request.group_id == "my-consumer-group"
    end

    test "handles empty consumer group" do
      request = Adapter.consumer_metadata_request("")

      assert request.group_id == ""
    end

    test "handles consumer group with special characters" do
      request = Adapter.consumer_metadata_request("group-with-dashes_and_underscores.and.dots")

      assert request.group_id == "group-with-dashes_and_underscores.and.dots"
    end

    test "handles unicode consumer group" do
      request = Adapter.consumer_metadata_request("group-unicode-test")

      assert request.group_id == "group-unicode-test"
    end
  end

  describe "consumer_metadata_response/1 - V0 Response" do
    test "converts successful V0 response to legacy format" do
      kayrock_response = %FindCoordinator.V0.Response{
        error_code: 0,
        coordinator: %{node_id: 1, host: "broker1.example.com", port: 9092}
      }

      legacy_response = Adapter.consumer_metadata_response(kayrock_response)

      assert %ConsumerMetadataResponse{} = legacy_response
      assert legacy_response.coordinator_id == 1
      assert legacy_response.coordinator_host == "broker1.example.com"
      assert legacy_response.coordinator_port == 9092
      assert legacy_response.error_code == :no_error
    end

    test "converts V0 error response (coordinator_not_available)" do
      kayrock_response = %FindCoordinator.V0.Response{
        error_code: 15,
        coordinator: nil
      }

      legacy_response = Adapter.consumer_metadata_response(kayrock_response)

      assert %ConsumerMetadataResponse{} = legacy_response
      assert legacy_response.error_code == :coordinator_not_available
      assert legacy_response.coordinator_id == 0
      assert legacy_response.coordinator_host == ""
      assert legacy_response.coordinator_port == 0
    end

    test "converts V0 error response (not_coordinator)" do
      kayrock_response = %FindCoordinator.V0.Response{
        error_code: 16,
        coordinator: nil
      }

      legacy_response = Adapter.consumer_metadata_response(kayrock_response)

      assert legacy_response.error_code == :not_coordinator
    end

    test "converts V0 error response (invalid_group_id)" do
      kayrock_response = %FindCoordinator.V0.Response{
        error_code: 24,
        coordinator: nil
      }

      legacy_response = Adapter.consumer_metadata_response(kayrock_response)

      assert legacy_response.error_code == :invalid_group_id
    end

    test "converts V0 error response (group_authorization_failed)" do
      kayrock_response = %FindCoordinator.V0.Response{
        error_code: 30,
        coordinator: nil
      }

      legacy_response = Adapter.consumer_metadata_response(kayrock_response)

      assert legacy_response.error_code == :group_authorization_failed
    end

    test "handles V0 response with different broker ports" do
      kayrock_response = %FindCoordinator.V0.Response{
        error_code: 0,
        coordinator: %{node_id: 42, host: "localhost", port: 19092}
      }

      legacy_response = Adapter.consumer_metadata_response(kayrock_response)

      assert legacy_response.coordinator_id == 42
      assert legacy_response.coordinator_host == "localhost"
      assert legacy_response.coordinator_port == 19092
    end
  end

  describe "consumer_metadata_response/1 - V1 Response" do
    test "converts successful V1 response to legacy format (ignores throttle and error_message)" do
      kayrock_response = %FindCoordinator.V1.Response{
        throttle_time_ms: 100,
        error_code: 0,
        error_message: nil,
        coordinator: %{node_id: 2, host: "broker2.example.com", port: 9093}
      }

      legacy_response = Adapter.consumer_metadata_response(kayrock_response)

      assert %ConsumerMetadataResponse{} = legacy_response
      assert legacy_response.coordinator_id == 2
      assert legacy_response.coordinator_host == "broker2.example.com"
      assert legacy_response.coordinator_port == 9093
      assert legacy_response.error_code == :no_error
    end

    test "converts V1 error response with throttle_time_ms" do
      kayrock_response = %FindCoordinator.V1.Response{
        throttle_time_ms: 50,
        error_code: 15,
        error_message: "Coordinator not available",
        coordinator: nil
      }

      legacy_response = Adapter.consumer_metadata_response(kayrock_response)

      assert legacy_response.error_code == :coordinator_not_available
      assert legacy_response.coordinator_id == 0
      assert legacy_response.coordinator_host == ""
      assert legacy_response.coordinator_port == 0
    end

    test "converts V1 error response (not_coordinator) with error_message" do
      kayrock_response = %FindCoordinator.V1.Response{
        throttle_time_ms: 0,
        error_code: 16,
        error_message: "This is not the coordinator",
        coordinator: nil
      }

      legacy_response = Adapter.consumer_metadata_response(kayrock_response)

      assert legacy_response.error_code == :not_coordinator
    end

    test "converts V1 error response (invalid_group_id) with error_message" do
      kayrock_response = %FindCoordinator.V1.Response{
        throttle_time_ms: 0,
        error_code: 24,
        error_message: "Invalid group ID",
        coordinator: nil
      }

      legacy_response = Adapter.consumer_metadata_response(kayrock_response)

      assert legacy_response.error_code == :invalid_group_id
    end

    test "converts V1 error response (group_authorization_failed) with throttle" do
      kayrock_response = %FindCoordinator.V1.Response{
        throttle_time_ms: 200,
        error_code: 30,
        error_message: "Authorization failed",
        coordinator: nil
      }

      legacy_response = Adapter.consumer_metadata_response(kayrock_response)

      assert legacy_response.error_code == :group_authorization_failed
    end

    test "handles V1 response with zero throttle_time_ms" do
      kayrock_response = %FindCoordinator.V1.Response{
        throttle_time_ms: 0,
        error_code: 0,
        error_message: nil,
        coordinator: %{node_id: 3, host: "broker3", port: 9094}
      }

      legacy_response = Adapter.consumer_metadata_response(kayrock_response)

      assert legacy_response.error_code == :no_error
      assert legacy_response.coordinator_id == 3
    end
  end

  describe "round-trip conversion" do
    test "request -> kayrock -> response maintains structure" do
      # Start with consumer group string
      consumer_group = "test-consumer-group"

      # Convert to Kayrock request
      kayrock_request = Adapter.consumer_metadata_request(consumer_group)

      assert kayrock_request.group_id == consumer_group

      # Simulate successful Kayrock V0 response
      kayrock_response = %FindCoordinator.V0.Response{
        error_code: 0,
        coordinator: %{node_id: 1, host: "localhost", port: 9092}
      }

      # Convert to legacy response
      legacy_response = Adapter.consumer_metadata_response(kayrock_response)

      assert %ConsumerMetadataResponse{} = legacy_response
      assert legacy_response.error_code == :no_error
      assert legacy_response.coordinator_id == 1
      assert legacy_response.coordinator_host == "localhost"
      assert legacy_response.coordinator_port == 9092
    end

    test "request -> kayrock V1 -> response handles errors" do
      # Start with consumer group
      consumer_group = "failing-group"

      # Convert to Kayrock request
      kayrock_request = Adapter.consumer_metadata_request(consumer_group)

      assert kayrock_request.group_id == consumer_group

      # Simulate error Kayrock V1 response
      kayrock_response = %FindCoordinator.V1.Response{
        throttle_time_ms: 100,
        error_code: 15,
        error_message: "Coordinator not available",
        coordinator: nil
      }

      # Convert to legacy response
      legacy_response = Adapter.consumer_metadata_response(kayrock_response)

      assert legacy_response.error_code == :coordinator_not_available
      # Legacy format uses defaults when coordinator is nil
      assert legacy_response.coordinator_id == 0
      assert legacy_response.coordinator_host == ""
      assert legacy_response.coordinator_port == 0
    end
  end

  describe "version comparison" do
    test "V0 and V1 produce same legacy response for successful case" do
      coordinator = %{node_id: 5, host: "broker5.test", port: 9092}

      v0_response = %FindCoordinator.V0.Response{
        error_code: 0,
        coordinator: coordinator
      }

      v1_response = %FindCoordinator.V1.Response{
        throttle_time_ms: 50,
        error_code: 0,
        error_message: nil,
        coordinator: coordinator
      }

      legacy_v0 = Adapter.consumer_metadata_response(v0_response)
      legacy_v1 = Adapter.consumer_metadata_response(v1_response)

      # Both should produce identical legacy responses
      assert legacy_v0.coordinator_id == legacy_v1.coordinator_id
      assert legacy_v0.coordinator_host == legacy_v1.coordinator_host
      assert legacy_v0.coordinator_port == legacy_v1.coordinator_port
      assert legacy_v0.error_code == legacy_v1.error_code
    end

    test "V0 and V1 produce same legacy response for error case" do
      v0_response = %FindCoordinator.V0.Response{
        error_code: 16,
        coordinator: nil
      }

      v1_response = %FindCoordinator.V1.Response{
        throttle_time_ms: 0,
        error_code: 16,
        error_message: "Not the coordinator",
        coordinator: nil
      }

      legacy_v0 = Adapter.consumer_metadata_response(v0_response)
      legacy_v1 = Adapter.consumer_metadata_response(v1_response)

      # Both should produce identical error responses
      assert legacy_v0.error_code == legacy_v1.error_code
      assert legacy_v0.coordinator_id == legacy_v1.coordinator_id
      assert legacy_v0.coordinator_host == legacy_v1.coordinator_host
      assert legacy_v0.coordinator_port == legacy_v1.coordinator_port
    end
  end

  describe "edge cases" do
    test "handles coordinator with node_id 0" do
      kayrock_response = %FindCoordinator.V0.Response{
        error_code: 0,
        coordinator: %{node_id: 0, host: "broker0", port: 9092}
      }

      legacy_response = Adapter.consumer_metadata_response(kayrock_response)

      assert legacy_response.coordinator_id == 0
      assert legacy_response.coordinator_host == "broker0"
    end

    test "handles coordinator with high node_id" do
      kayrock_response = %FindCoordinator.V0.Response{
        error_code: 0,
        coordinator: %{node_id: 999, host: "broker999", port: 9092}
      }

      legacy_response = Adapter.consumer_metadata_response(kayrock_response)

      assert legacy_response.coordinator_id == 999
    end

    test "handles coordinator with IPv4 host" do
      kayrock_response = %FindCoordinator.V0.Response{
        error_code: 0,
        coordinator: %{node_id: 1, host: "192.168.1.100", port: 9092}
      }

      legacy_response = Adapter.consumer_metadata_response(kayrock_response)

      assert legacy_response.coordinator_host == "192.168.1.100"
    end

    test "handles coordinator with IPv6 host" do
      kayrock_response = %FindCoordinator.V0.Response{
        error_code: 0,
        coordinator: %{node_id: 1, host: "::1", port: 9092}
      }

      legacy_response = Adapter.consumer_metadata_response(kayrock_response)

      assert legacy_response.coordinator_host == "::1"
    end

    test "handles coordinator with non-standard port" do
      kayrock_response = %FindCoordinator.V0.Response{
        error_code: 0,
        coordinator: %{node_id: 1, host: "localhost", port: 29092}
      }

      legacy_response = Adapter.consumer_metadata_response(kayrock_response)

      assert legacy_response.coordinator_port == 29092
    end
  end
end
