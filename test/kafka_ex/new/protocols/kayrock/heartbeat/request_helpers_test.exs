defmodule KafkaEx.New.Protocols.Kayrock.Heartbeat.RequestHelpersTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Protocols.Kayrock.Heartbeat.RequestHelpers

  describe "extract_common_fields/1" do
    test "extracts all required fields from opts" do
      opts = [
        group_id: "test-group",
        member_id: "member-123",
        generation_id: 5,
        other: "value"
      ]

      result = RequestHelpers.extract_common_fields(opts)

      assert %{
               group_id: "test-group",
               member_id: "member-123",
               generation_id: 5
             } = result
    end

    test "extracts fields with generation_id zero" do
      opts = [
        group_id: "new-group",
        member_id: "new-member",
        generation_id: 0
      ]

      result = RequestHelpers.extract_common_fields(opts)

      assert result.generation_id == 0
    end

    test "extracts fields with large generation_id" do
      opts = [
        group_id: "test-group",
        member_id: "test-member",
        generation_id: 999_999
      ]

      result = RequestHelpers.extract_common_fields(opts)

      assert result.generation_id == 999_999
    end

    test "raises when group_id is missing" do
      opts = [member_id: "member-123", generation_id: 5]

      assert_raise KeyError, fn ->
        RequestHelpers.extract_common_fields(opts)
      end
    end

    test "raises when member_id is missing" do
      opts = [group_id: "test-group", generation_id: 5]

      assert_raise KeyError, fn ->
        RequestHelpers.extract_common_fields(opts)
      end
    end

    test "raises when generation_id is missing" do
      opts = [group_id: "test-group", member_id: "member-123"]

      assert_raise KeyError, fn ->
        RequestHelpers.extract_common_fields(opts)
      end
    end

    test "ignores extra fields in opts" do
      opts = [
        group_id: "test-group",
        member_id: "member-123",
        generation_id: 5,
        extra_field: "ignored",
        another: 42
      ]

      result = RequestHelpers.extract_common_fields(opts)

      assert Enum.sort(Map.keys(result)) == [:generation_id, :group_id, :member_id]
    end
  end
end
