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

  describe "V2 Response implementation" do
    test "parses successful response with all fields" do
      response = %Kayrock.FindCoordinator.V2.Response{
        throttle_time_ms: 25,
        error_code: 0,
        error_message: nil,
        node_id: 3,
        host: "broker3.example.com",
        port: 9094
      }

      assert {:ok, %FindCoordinator{} = result} = Response.parse_response(response)

      assert result.error_code == :no_error
      assert result.throttle_time_ms == 25
      assert is_nil(result.error_message)
      assert result.coordinator.node_id == 3
      assert result.coordinator.host == "broker3.example.com"
      assert result.coordinator.port == 9094
    end

    test "parses response with error" do
      response = %Kayrock.FindCoordinator.V2.Response{
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
      response = %Kayrock.FindCoordinator.V2.Response{
        throttle_time_ms: 500,
        error_code: 0,
        error_message: nil,
        node_id: 1,
        host: "localhost",
        port: 9092
      }

      assert {:ok, result} = Response.parse_response(response)
      assert result.throttle_time_ms == 500
    end

    test "parses response with not_coordinator error" do
      response = %Kayrock.FindCoordinator.V2.Response{
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
  end

  describe "V3 Response implementation (flexible version)" do
    test "parses successful response with all fields" do
      response = %Kayrock.FindCoordinator.V3.Response{
        throttle_time_ms: 10,
        error_code: 0,
        error_message: nil,
        node_id: 4,
        host: "broker4.example.com",
        port: 9095,
        tagged_fields: []
      }

      assert {:ok, %FindCoordinator{} = result} = Response.parse_response(response)

      assert result.error_code == :no_error
      assert result.throttle_time_ms == 10
      assert is_nil(result.error_message)
      assert result.coordinator.node_id == 4
      assert result.coordinator.host == "broker4.example.com"
      assert result.coordinator.port == 9095
    end

    test "parses response with error" do
      response = %Kayrock.FindCoordinator.V3.Response{
        throttle_time_ms: 0,
        error_code: 15,
        error_message: "Coordinator not available",
        node_id: nil,
        host: nil,
        port: nil,
        tagged_fields: []
      }

      assert {:error, error} = Response.parse_response(response)
      assert error.error == :coordinator_not_available
    end

    test "parses response with throttling" do
      response = %Kayrock.FindCoordinator.V3.Response{
        throttle_time_ms: 2000,
        error_code: 0,
        error_message: nil,
        node_id: 1,
        host: "localhost",
        port: 9092,
        tagged_fields: []
      }

      assert {:ok, result} = Response.parse_response(response)
      assert result.throttle_time_ms == 2000
    end

    test "parses response with unknown tagged_fields (forward compat)" do
      # Unknown tagged fields should not affect parsing
      response = %Kayrock.FindCoordinator.V3.Response{
        throttle_time_ms: 0,
        error_code: 0,
        error_message: nil,
        node_id: 7,
        host: "broker7.example.com",
        port: 9092,
        tagged_fields: [{99, <<1, 2, 3>>}]
      }

      assert {:ok, result} = Response.parse_response(response)
      assert result.coordinator.node_id == 7
      assert result.coordinator.host == "broker7.example.com"
    end

    test "parses response for transaction coordinator" do
      response = %Kayrock.FindCoordinator.V3.Response{
        throttle_time_ms: 0,
        error_code: 0,
        error_message: nil,
        node_id: 8,
        host: "tx-broker.example.com",
        port: 9092,
        tagged_fields: []
      }

      assert {:ok, result} = Response.parse_response(response)
      assert result.coordinator.node_id == 8
      assert result.coordinator.host == "tx-broker.example.com"
    end

    test "parses response with group_authorization_failed error" do
      response = %Kayrock.FindCoordinator.V3.Response{
        throttle_time_ms: 0,
        error_code: 30,
        error_message: "Authorization failed",
        node_id: nil,
        host: nil,
        port: nil,
        tagged_fields: []
      }

      assert {:error, error} = Response.parse_response(response)
      assert error.error == :group_authorization_failed
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

  describe "Version comparison (V0 vs V1 vs V2 vs V3)" do
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

    test "V2 response is identical to V1 in domain fields" do
      v2_response = %Kayrock.FindCoordinator.V2.Response{
        throttle_time_ms: 200,
        error_code: 0,
        error_message: nil,
        node_id: 1,
        host: "localhost",
        port: 9092
      }

      assert {:ok, result} = Response.parse_response(v2_response)
      assert result.throttle_time_ms == 200
      assert result.coordinator.node_id == 1
    end

    test "V3 response is identical to V1/V2 in domain fields" do
      v3_response = %Kayrock.FindCoordinator.V3.Response{
        throttle_time_ms: 300,
        error_code: 0,
        error_message: nil,
        node_id: 1,
        host: "localhost",
        port: 9092,
        tagged_fields: []
      }

      assert {:ok, result} = Response.parse_response(v3_response)
      assert result.throttle_time_ms == 300
      assert result.coordinator.node_id == 1
    end

    test "all V1+ versions produce consistent domain results" do
      common_fields = [
        node_id: 42,
        host: "consistency-broker.example.com",
        port: 19092,
        error_code: 0,
        error_message: nil,
        throttle_time_ms: 77
      ]

      responses = [
        {"V1", struct(Kayrock.FindCoordinator.V1.Response, common_fields)},
        {"V2", struct(Kayrock.FindCoordinator.V2.Response, common_fields)},
        {"V3", struct(Kayrock.FindCoordinator.V3.Response, Keyword.put(common_fields, :tagged_fields, []))}
      ]

      for {version_label, response} <- responses do
        assert {:ok, result} = Response.parse_response(response),
               "#{version_label}: expected {:ok, _}"

        assert result.error_code == :no_error,
               "#{version_label}: expected :no_error"

        assert result.throttle_time_ms == 77,
               "#{version_label}: expected throttle_time_ms 77"

        assert result.coordinator.node_id == 42,
               "#{version_label}: expected node_id 42"

        assert result.coordinator.host == "consistency-broker.example.com",
               "#{version_label}: expected host 'consistency-broker.example.com'"

        assert result.coordinator.port == 19092,
               "#{version_label}: expected port 19092"
      end
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

  describe "Any Response fallback implementation" do
    test "handles V0-like map via Any fallback (no throttle_time_ms)" do
      # Simulate an unknown version with V0 fields
      response = %{
        error_code: 0,
        node_id: 10,
        host: "any-broker.example.com",
        port: 9092
      }

      assert {:ok, result} = Response.parse_response(response)
      assert result.coordinator.node_id == 10
      assert result.coordinator.host == "any-broker.example.com"
      assert is_nil(result.throttle_time_ms)
    end

    test "handles V1-like map via Any fallback (has throttle_time_ms)" do
      # Simulate an unknown version with V1+ fields
      response = %{
        throttle_time_ms: 42,
        error_code: 0,
        error_message: nil,
        node_id: 11,
        host: "any-v1-broker.example.com",
        port: 9093
      }

      assert {:ok, result} = Response.parse_response(response)
      assert result.coordinator.node_id == 11
      assert result.throttle_time_ms == 42
    end

    test "handles V3-like map via Any fallback (has tagged_fields)" do
      # Simulate an unknown flexible version
      response = %{
        throttle_time_ms: 55,
        error_code: 0,
        error_message: nil,
        node_id: 12,
        host: "any-flex-broker.example.com",
        port: 9094,
        tagged_fields: []
      }

      assert {:ok, result} = Response.parse_response(response)
      assert result.coordinator.node_id == 12
      assert result.throttle_time_ms == 55
    end

    test "handles error in V0-like map via Any fallback" do
      response = %{
        error_code: 15,
        node_id: nil,
        host: nil,
        port: nil
      }

      assert {:error, error} = Response.parse_response(response)
      assert error.error == :coordinator_not_available
    end

    test "handles error in V1-like map via Any fallback" do
      response = %{
        throttle_time_ms: 0,
        error_code: 16,
        error_message: "Not coordinator",
        node_id: nil,
        host: nil,
        port: nil
      }

      assert {:error, error} = Response.parse_response(response)
      assert error.error == :not_coordinator
    end
  end
end
