defmodule KafkaEx.Protocol.Kayrock.FindCoordinator.RequestHelpersTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Protocol.Kayrock.FindCoordinator.RequestHelpers

  describe "extract_group_id/1" do
    test "extracts group_id from options" do
      opts = [group_id: "my-consumer-group"]

      result = RequestHelpers.extract_group_id(opts)

      assert result == "my-consumer-group"
    end

    test "raises on missing group_id" do
      assert_raise KeyError, fn ->
        RequestHelpers.extract_group_id([])
      end
    end
  end

  describe "extract_v1_fields/1" do
    test "extracts coordinator_key and defaults to group coordinator type" do
      opts = [group_id: "my-group"]

      {key, type} = RequestHelpers.extract_v1_fields(opts)

      assert key == "my-group"
      assert type == 0
    end

    test "uses coordinator_key when provided" do
      opts = [coordinator_key: "my-coordinator-key", group_id: "fallback"]

      {key, _type} = RequestHelpers.extract_v1_fields(opts)

      assert key == "my-coordinator-key"
    end

    test "extracts transaction coordinator type as integer" do
      opts = [group_id: "tx-id", coordinator_type: 1]

      {_key, type} = RequestHelpers.extract_v1_fields(opts)

      assert type == 1
    end

    test "extracts transaction coordinator type as atom" do
      opts = [group_id: "tx-id", coordinator_type: :transaction]

      {_key, type} = RequestHelpers.extract_v1_fields(opts)

      assert type == 1
    end

    test "extracts group coordinator type as atom" do
      opts = [group_id: "group-id", coordinator_type: :group]

      {_key, type} = RequestHelpers.extract_v1_fields(opts)

      assert type == 0
    end
  end

  describe "extract_coordinator_type/1" do
    test "returns 0 for :group" do
      assert RequestHelpers.extract_coordinator_type(coordinator_type: :group) == 0
    end

    test "returns 1 for :transaction" do
      assert RequestHelpers.extract_coordinator_type(coordinator_type: :transaction) == 1
    end

    test "returns 0 for integer 0" do
      assert RequestHelpers.extract_coordinator_type(coordinator_type: 0) == 0
    end

    test "returns 1 for integer 1" do
      assert RequestHelpers.extract_coordinator_type(coordinator_type: 1) == 1
    end

    test "defaults to 0 when not provided" do
      assert RequestHelpers.extract_coordinator_type([]) == 0
    end
  end
end
