defmodule KafkaEx.Protocol.Kayrock.Heartbeat.RequestHelpersTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Protocol.Kayrock.Heartbeat.RequestHelpers

  # ---- extract_common_fields/1 ----

  describe "extract_common_fields/1" do
    test "extracts all required fields" do
      opts = [
        group_id: "my-consumer-group",
        member_id: "member-123",
        generation_id: 5
      ]

      result = RequestHelpers.extract_common_fields(opts)

      assert result.group_id == "my-consumer-group"
      assert result.member_id == "member-123"
      assert result.generation_id == 5
    end

    test "raises on missing group_id" do
      assert_raise KeyError, fn ->
        RequestHelpers.extract_common_fields(member_id: "member", generation_id: 1)
      end
    end

    test "raises on missing member_id" do
      assert_raise KeyError, fn ->
        RequestHelpers.extract_common_fields(group_id: "group", generation_id: 1)
      end
    end

    test "raises on missing generation_id" do
      assert_raise KeyError, fn ->
        RequestHelpers.extract_common_fields(group_id: "group", member_id: "member")
      end
    end

    test "ignores extra fields" do
      opts = [
        group_id: "g",
        member_id: "m",
        generation_id: 0,
        extra: "ignored"
      ]

      result = RequestHelpers.extract_common_fields(opts)

      assert Enum.sort(Map.keys(result)) == [:generation_id, :group_id, :member_id]
    end
  end

  # ---- build_request_from_template/2 ----

  describe "build_request_from_template/2" do
    test "populates template with common fields" do
      template = %{group_id: nil, member_id: nil, generation_id: nil}

      opts = [
        group_id: "test-group",
        member_id: "member-123",
        generation_id: 5
      ]

      result = RequestHelpers.build_request_from_template(template, opts)

      assert result.group_id == "test-group"
      assert result.member_id == "member-123"
      assert result.generation_id == 5
    end

    test "preserves other fields in template" do
      template = %{
        group_id: nil,
        member_id: nil,
        generation_id: nil,
        correlation_id: 42,
        client_id: "my-client"
      }

      opts = [
        group_id: "test-group",
        member_id: "member-123",
        generation_id: 5
      ]

      result = RequestHelpers.build_request_from_template(template, opts)

      assert result.correlation_id == 42
      assert result.client_id == "my-client"
    end

    test "handles empty strings" do
      template = %{group_id: nil, member_id: nil, generation_id: nil}

      opts = [
        group_id: "",
        member_id: "",
        generation_id: 0
      ]

      result = RequestHelpers.build_request_from_template(template, opts)

      assert result.group_id == ""
      assert result.member_id == ""
      assert result.generation_id == 0
    end

    test "handles large generation_id" do
      template = %{group_id: nil, member_id: nil, generation_id: nil}

      opts = [
        group_id: "test-group",
        member_id: "test-member",
        generation_id: 2_147_483_647
      ]

      result = RequestHelpers.build_request_from_template(template, opts)

      assert result.generation_id == 2_147_483_647
    end

    test "raises when required fields missing" do
      template = %{group_id: nil, member_id: nil, generation_id: nil}

      assert_raise KeyError, fn ->
        RequestHelpers.build_request_from_template(template, [])
      end
    end

    test "works with plain map template" do
      template = %{}

      opts = [
        group_id: "test-group",
        member_id: "member-123",
        generation_id: 5
      ]

      result = RequestHelpers.build_request_from_template(template, opts)

      assert result.group_id == "test-group"
      assert result.member_id == "member-123"
      assert result.generation_id == 5
    end
  end

  # ---- build_v3_plus_request/2 ----

  describe "build_v3_plus_request/2" do
    test "populates common fields plus group_instance_id" do
      template = %{
        group_id: nil,
        member_id: nil,
        generation_id: nil,
        group_instance_id: nil
      }

      opts = [
        group_id: "test-group",
        member_id: "member-123",
        generation_id: 5,
        group_instance_id: "static-instance-1"
      ]

      result = RequestHelpers.build_v3_plus_request(template, opts)

      assert result.group_id == "test-group"
      assert result.member_id == "member-123"
      assert result.generation_id == 5
      assert result.group_instance_id == "static-instance-1"
    end

    test "defaults group_instance_id to nil when not provided" do
      template = %{
        group_id: nil,
        member_id: nil,
        generation_id: nil,
        group_instance_id: nil
      }

      opts = [
        group_id: "test-group",
        member_id: "member-123",
        generation_id: 5
      ]

      result = RequestHelpers.build_v3_plus_request(template, opts)

      assert result.group_instance_id == nil
    end

    test "preserves other fields in template" do
      template = %{
        group_id: nil,
        member_id: nil,
        generation_id: nil,
        group_instance_id: nil,
        correlation_id: 99,
        client_id: "v3-client"
      }

      opts = [
        group_id: "test-group",
        member_id: "member-123",
        generation_id: 5,
        group_instance_id: "instance-id"
      ]

      result = RequestHelpers.build_v3_plus_request(template, opts)

      assert result.correlation_id == 99
      assert result.client_id == "v3-client"
    end

    test "works with empty map template" do
      template = %{}

      opts = [
        group_id: "test-group",
        member_id: "member-123",
        generation_id: 5,
        group_instance_id: "static-1"
      ]

      result = RequestHelpers.build_v3_plus_request(template, opts)

      assert result.group_id == "test-group"
      assert result.group_instance_id == "static-1"
    end

    test "handles empty strings for identifiers" do
      template = %{group_id: nil, member_id: nil, generation_id: nil, group_instance_id: nil}

      opts = [
        group_id: "",
        member_id: "",
        generation_id: 0,
        group_instance_id: ""
      ]

      result = RequestHelpers.build_v3_plus_request(template, opts)

      assert result.group_id == ""
      assert result.member_id == ""
      assert result.generation_id == 0
      assert result.group_instance_id == ""
    end

    test "raises when required common fields missing" do
      template = %{group_instance_id: nil}

      assert_raise KeyError, fn ->
        RequestHelpers.build_v3_plus_request(template, group_instance_id: "id")
      end
    end
  end
end
