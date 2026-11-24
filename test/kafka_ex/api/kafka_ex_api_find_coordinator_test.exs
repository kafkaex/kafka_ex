defmodule KafkaEx.APIFindCoordinatorTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Client.RequestBuilder
  alias KafkaEx.Client.State

  # FindCoordinator API key is 10
  @find_coordinator_api_key 10

  describe "RequestBuilder.build(:find_coordinator, ...)" do
    test "builds V0 request with group_id" do
      state = %State{api_versions: %{@find_coordinator_api_key => {0, 1}}}

      opts = [group_id: "test-group", api_version: 0]

      assert {:ok, request} = RequestBuilder.build(:find_coordinator, opts, state)
      assert request.key == "test-group"
    end

    test "builds V1 request with group_id" do
      state = %State{api_versions: %{@find_coordinator_api_key => {0, 1}}}

      opts = [group_id: "test-group", api_version: 1]

      assert {:ok, request} = RequestBuilder.build(:find_coordinator, opts, state)
      assert request.key == "test-group"
      assert request.key_type == 0
    end

    test "builds V1 request for transaction coordinator" do
      state = %State{api_versions: %{@find_coordinator_api_key => {0, 1}}}

      opts = [group_id: "tx-id", coordinator_type: :transaction, api_version: 1]

      assert {:ok, request} = RequestBuilder.build(:find_coordinator, opts, state)
      assert request.key == "tx-id"
      assert request.key_type == 1
    end

    test "returns error when api_version is not supported" do
      state = %State{api_versions: %{@find_coordinator_api_key => {0, 0}}}

      opts = [group_id: "test-group", api_version: 1]

      assert {:error, :api_version_no_supported} = RequestBuilder.build(:find_coordinator, opts, state)
    end
  end
end
