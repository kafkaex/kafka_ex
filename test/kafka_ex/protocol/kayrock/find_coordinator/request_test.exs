defmodule KafkaEx.Protocol.Kayrock.FindCoordinator.RequestTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Protocol.Kayrock.FindCoordinator.Request

  describe "V0 Request implementation" do
    test "builds V0 request with group_id" do
      template = %Kayrock.FindCoordinator.V0.Request{}

      opts = [group_id: "my-consumer-group"]

      result = Request.build_request(template, opts)

      assert result.key == "my-consumer-group"
    end

    test "raises when group_id is missing" do
      template = %Kayrock.FindCoordinator.V0.Request{}

      assert_raise KeyError, fn ->
        Request.build_request(template, [])
      end
    end
  end

  describe "V1 Request implementation" do
    test "builds V1 request with group_id (coordinator_type defaults to 0)" do
      template = %Kayrock.FindCoordinator.V1.Request{}

      opts = [group_id: "my-consumer-group"]

      result = Request.build_request(template, opts)

      assert result.key == "my-consumer-group"
      assert result.key_type == 0
    end

    test "builds V1 request with coordinator_key" do
      template = %Kayrock.FindCoordinator.V1.Request{}

      opts = [coordinator_key: "my-transactional-id", coordinator_type: 1]

      result = Request.build_request(template, opts)

      assert result.key == "my-transactional-id"
      assert result.key_type == 1
    end

    test "builds V1 request for group coordinator (type = 0)" do
      template = %Kayrock.FindCoordinator.V1.Request{}

      opts = [group_id: "consumer-group", coordinator_type: 0]

      result = Request.build_request(template, opts)

      assert result.key == "consumer-group"
      assert result.key_type == 0
    end

    test "builds V1 request for transaction coordinator (type = 1)" do
      template = %Kayrock.FindCoordinator.V1.Request{}

      opts = [group_id: "transactional-id", coordinator_type: 1]

      result = Request.build_request(template, opts)

      assert result.key == "transactional-id"
      assert result.key_type == 1
    end

    test "accepts :group atom for coordinator_type" do
      template = %Kayrock.FindCoordinator.V1.Request{}

      opts = [group_id: "my-group", coordinator_type: :group]

      result = Request.build_request(template, opts)

      assert result.key_type == 0
    end

    test "accepts :transaction atom for coordinator_type" do
      template = %Kayrock.FindCoordinator.V1.Request{}

      opts = [group_id: "my-tx-id", coordinator_type: :transaction]

      result = Request.build_request(template, opts)

      assert result.key_type == 1
    end

    test "coordinator_key takes precedence over group_id" do
      template = %Kayrock.FindCoordinator.V1.Request{}

      opts = [group_id: "group-id", coordinator_key: "coordinator-key"]

      result = Request.build_request(template, opts)

      assert result.key == "coordinator-key"
    end
  end

  describe "Version comparison" do
    test "V0 uses key field" do
      v0 = Request.build_request(%Kayrock.FindCoordinator.V0.Request{}, group_id: "test-group")

      assert v0.key == "test-group"
    end

    test "V1 uses key and key_type fields" do
      v1 = Request.build_request(%Kayrock.FindCoordinator.V1.Request{}, group_id: "test-group")

      assert v1.key == "test-group"
      assert v1.key_type == 0
    end
  end
end
