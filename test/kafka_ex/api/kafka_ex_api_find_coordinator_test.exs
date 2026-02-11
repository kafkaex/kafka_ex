defmodule KafkaEx.APIFindCoordinatorTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Client.RequestBuilder
  alias KafkaEx.Client.ResponseParser
  alias KafkaEx.Client.State
  alias KafkaEx.Test.KayrockFixtures, as: Fixtures

  # FindCoordinator API key is 10
  @find_coordinator_api_key 10

  describe "RequestBuilder.find_coordinator_request/2" do
    test "builds V0 request with group_id" do
      state = %State{api_versions: %{@find_coordinator_api_key => {0, 1}}}

      opts = [group_id: "test-group", api_version: 0]

      assert {:ok, request} = RequestBuilder.find_coordinator_request(opts, state)
      assert request.key == "test-group"
    end

    test "builds V1 request with group_id" do
      state = %State{api_versions: %{@find_coordinator_api_key => {0, 1}}}

      opts = [group_id: "test-group", api_version: 1]

      assert {:ok, request} = RequestBuilder.find_coordinator_request(opts, state)
      assert request.key == "test-group"
      assert request.key_type == 0
    end

    test "builds V1 request for transaction coordinator" do
      state = %State{api_versions: %{@find_coordinator_api_key => {0, 1}}}

      opts = [group_id: "tx-id", coordinator_type: :transaction, api_version: 1]

      assert {:ok, request} = RequestBuilder.find_coordinator_request(opts, state)
      assert request.key == "tx-id"
      assert request.key_type == 1
    end

    test "returns error when api_version is not supported" do
      state = %State{api_versions: %{@find_coordinator_api_key => {0, 0}}}

      opts = [group_id: "test-group", api_version: 1]

      assert {:error, :api_version_no_supported} = RequestBuilder.find_coordinator_request(opts, state)
    end
  end

  describe "ResponseParser.find_coordinator_response/1" do
    test "parses successful V0 response" do
      response = Fixtures.build_response(:find_coordinator, 0,
        error_code: 0,
        node_id: 1,
        host: "localhost",
        port: 9092
      )

      assert {:ok, result} = ResponseParser.find_coordinator_response(response)
      assert result.error_code == :no_error
      assert result.coordinator.node_id == 1
    end

    test "parses successful V1 response" do
      response = Fixtures.build_response(:find_coordinator, 1,
        throttle_time_ms: 50,
        error_code: 0,
        error_message: nil,
        node_id: 2,
        host: "broker2",
        port: 9093
      )

      assert {:ok, result} = ResponseParser.find_coordinator_response(response)
      assert result.error_code == :no_error
      assert result.throttle_time_ms == 50
      assert result.coordinator.node_id == 2
    end

    test "returns error for failed response" do
      response = Fixtures.build_response(:find_coordinator, 1,
        throttle_time_ms: 0,
        error_code: 15,
        error_message: "Coordinator not available",
        node_id: nil,
        host: nil,
        port: nil
      )

      assert {:error, error} = ResponseParser.find_coordinator_response(response)
      assert error.error == :coordinator_not_available
    end
  end
end
