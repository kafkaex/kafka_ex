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

  describe "V2 Request implementation" do
    test "builds V2 request with group_id (coordinator_type defaults to 0)" do
      template = %Kayrock.FindCoordinator.V2.Request{}

      opts = [group_id: "my-consumer-group"]

      result = Request.build_request(template, opts)

      assert result.key == "my-consumer-group"
      assert result.key_type == 0
    end

    test "builds V2 request with coordinator_key and transaction type" do
      template = %Kayrock.FindCoordinator.V2.Request{}

      opts = [coordinator_key: "my-transactional-id", coordinator_type: 1]

      result = Request.build_request(template, opts)

      assert result.key == "my-transactional-id"
      assert result.key_type == 1
    end

    test "accepts :group atom for coordinator_type" do
      template = %Kayrock.FindCoordinator.V2.Request{}

      opts = [group_id: "my-group", coordinator_type: :group]

      result = Request.build_request(template, opts)

      assert result.key_type == 0
    end

    test "accepts :transaction atom for coordinator_type" do
      template = %Kayrock.FindCoordinator.V2.Request{}

      opts = [group_id: "my-tx-id", coordinator_type: :transaction]

      result = Request.build_request(template, opts)

      assert result.key_type == 1
    end

    test "coordinator_key takes precedence over group_id" do
      template = %Kayrock.FindCoordinator.V2.Request{}

      opts = [group_id: "group-id", coordinator_key: "coordinator-key"]

      result = Request.build_request(template, opts)

      assert result.key == "coordinator-key"
    end

    test "raises when neither coordinator_key nor group_id provided" do
      template = %Kayrock.FindCoordinator.V2.Request{}

      assert_raise KeyError, fn ->
        Request.build_request(template, coordinator_type: 0)
      end
    end
  end

  describe "V3 Request implementation (flexible version)" do
    test "builds V3 request with group_id (coordinator_type defaults to 0)" do
      template = %Kayrock.FindCoordinator.V3.Request{}

      opts = [group_id: "my-consumer-group"]

      result = Request.build_request(template, opts)

      assert result.key == "my-consumer-group"
      assert result.key_type == 0
      # V3 struct has tagged_fields but build_request does not modify them
      assert result.tagged_fields == []
    end

    test "builds V3 request with coordinator_key and transaction type" do
      template = %Kayrock.FindCoordinator.V3.Request{}

      opts = [coordinator_key: "my-transactional-id", coordinator_type: 1]

      result = Request.build_request(template, opts)

      assert result.key == "my-transactional-id"
      assert result.key_type == 1
    end

    test "accepts :group atom for coordinator_type" do
      template = %Kayrock.FindCoordinator.V3.Request{}

      opts = [group_id: "my-group", coordinator_type: :group]

      result = Request.build_request(template, opts)

      assert result.key_type == 0
    end

    test "accepts :transaction atom for coordinator_type" do
      template = %Kayrock.FindCoordinator.V3.Request{}

      opts = [group_id: "my-tx-id", coordinator_type: :transaction]

      result = Request.build_request(template, opts)

      assert result.key_type == 1
    end

    test "coordinator_key takes precedence over group_id" do
      template = %Kayrock.FindCoordinator.V3.Request{}

      opts = [group_id: "group-id", coordinator_key: "coordinator-key"]

      result = Request.build_request(template, opts)

      assert result.key == "coordinator-key"
    end

    test "preserves tagged_fields from template" do
      template = %Kayrock.FindCoordinator.V3.Request{tagged_fields: [{0, <<1, 2, 3>>}]}

      opts = [group_id: "my-group"]

      result = Request.build_request(template, opts)

      assert result.tagged_fields == [{0, <<1, 2, 3>>}]
    end

    test "raises when neither coordinator_key nor group_id provided" do
      template = %Kayrock.FindCoordinator.V3.Request{}

      assert_raise KeyError, fn ->
        Request.build_request(template, coordinator_type: 0)
      end
    end

    test "V3 request struct can be serialized by Kayrock after build" do
      template = %Kayrock.FindCoordinator.V3.Request{
        correlation_id: 1,
        client_id: "test-client"
      }

      result = Request.build_request(template, group_id: "test-group")

      assert result.key == "test-group"
      assert result.key_type == 0
      assert result.correlation_id == 1
      assert result.client_id == "test-client"
      # Verify Kayrock can serialize without error
      assert Kayrock.FindCoordinator.V3.Request.serialize(result)
    end
  end

  describe "Cross-version consistency (V1-V3)" do
    @consistency_cases [
      {"V1", %Kayrock.FindCoordinator.V1.Request{}},
      {"V2", %Kayrock.FindCoordinator.V2.Request{}},
      {"V3", %Kayrock.FindCoordinator.V3.Request{}}
    ]

    test "all V1+ versions produce same key and key_type for group coordinator" do
      opts = [group_id: "consistency-group", coordinator_type: 0]

      for {version_label, template} <- @consistency_cases do
        result = Request.build_request(template, opts)

        assert result.key == "consistency-group",
               "#{version_label}: expected key 'consistency-group', got '#{result.key}'"

        assert result.key_type == 0,
               "#{version_label}: expected key_type 0, got #{result.key_type}"
      end
    end

    test "all V1+ versions produce same key and key_type for transaction coordinator" do
      opts = [coordinator_key: "tx-consistency", coordinator_type: :transaction]

      for {version_label, template} <- @consistency_cases do
        result = Request.build_request(template, opts)

        assert result.key == "tx-consistency",
               "#{version_label}: expected key 'tx-consistency', got '#{result.key}'"

        assert result.key_type == 1,
               "#{version_label}: expected key_type 1, got #{result.key_type}"
      end
    end

    test "all V1+ versions default coordinator_type to 0 when not specified" do
      opts = [group_id: "default-type-group"]

      for {version_label, template} <- @consistency_cases do
        result = Request.build_request(template, opts)

        assert result.key_type == 0,
               "#{version_label}: expected default key_type 0, got #{result.key_type}"
      end
    end
  end

  describe "Version comparison" do
    test "V0 uses key field only (no key_type)" do
      v0 = Request.build_request(%Kayrock.FindCoordinator.V0.Request{}, group_id: "test-group")

      assert v0.key == "test-group"
      refute Map.has_key?(v0, :key_type)
    end

    test "V1 uses key and key_type fields" do
      v1 = Request.build_request(%Kayrock.FindCoordinator.V1.Request{}, group_id: "test-group")

      assert v1.key == "test-group"
      assert v1.key_type == 0
    end

    test "V2 uses key and key_type fields (same as V1)" do
      v2 = Request.build_request(%Kayrock.FindCoordinator.V2.Request{}, group_id: "test-group")

      assert v2.key == "test-group"
      assert v2.key_type == 0
    end

    test "V3 uses key, key_type, and has tagged_fields" do
      v3 = Request.build_request(%Kayrock.FindCoordinator.V3.Request{}, group_id: "test-group")

      assert v3.key == "test-group"
      assert v3.key_type == 0
      assert v3.tagged_fields == []
    end
  end

  describe "Any Request fallback implementation" do
    test "handles V0-like struct via Any fallback (only key, no key_type)" do
      # Simulate an unknown version with only V0 fields
      request = %{key: nil, correlation_id: nil, client_id: nil}

      opts = [group_id: "any-v0-group"]

      result = Request.build_request(request, opts)

      assert result.key == "any-v0-group"
      refute Map.has_key?(result, :key_type)
    end

    test "handles V1-like struct via Any fallback (has key_type)" do
      # Simulate an unknown version with V1+ fields
      request = %{key: nil, key_type: nil, correlation_id: nil, client_id: nil}

      opts = [group_id: "any-v1-group", coordinator_type: 0]

      result = Request.build_request(request, opts)

      assert result.key == "any-v1-group"
      assert result.key_type == 0
    end

    test "handles V3-like struct via Any fallback (has tagged_fields)" do
      # Simulate an unknown flexible version
      request = %{key: nil, key_type: nil, tagged_fields: [], correlation_id: nil, client_id: nil}

      opts = [coordinator_key: "any-tx-id", coordinator_type: :transaction]

      result = Request.build_request(request, opts)

      assert result.key == "any-tx-id"
      assert result.key_type == 1
    end

    test "Any fallback V1+ path defaults coordinator_type to 0" do
      request = %{key: nil, key_type: nil, correlation_id: nil, client_id: nil}

      opts = [group_id: "any-default-group"]

      result = Request.build_request(request, opts)

      assert result.key_type == 0
    end

    test "Any fallback V0 path raises when group_id is missing" do
      request = %{key: nil, correlation_id: nil, client_id: nil}

      assert_raise KeyError, fn ->
        Request.build_request(request, [])
      end
    end

    test "Any fallback V1+ path raises when neither coordinator_key nor group_id provided" do
      request = %{key: nil, key_type: nil, correlation_id: nil, client_id: nil}

      assert_raise KeyError, fn ->
        Request.build_request(request, coordinator_type: 0)
      end
    end
  end
end
