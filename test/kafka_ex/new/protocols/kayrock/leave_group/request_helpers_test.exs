defmodule KafkaEx.New.Protocols.Kayrock.LeaveGroup.RequestHelpersTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Protocols.Kayrock.LeaveGroup.RequestHelpers

  describe "extract_common_fields/1" do
    test "extracts all required fields from opts" do
      opts = [
        group_id: "test-group",
        member_id: "member-123",
        other: "value"
      ]

      result = RequestHelpers.extract_common_fields(opts)

      assert %{
               group_id: "test-group",
               member_id: "member-123"
             } = result
    end

    test "extracts fields with empty strings" do
      opts = [
        group_id: "",
        member_id: ""
      ]

      result = RequestHelpers.extract_common_fields(opts)

      assert result.group_id == ""
      assert result.member_id == ""
    end

    test "extracts fields with long strings" do
      long_group = String.duplicate("a", 1000)
      long_member = String.duplicate("b", 1000)

      opts = [
        group_id: long_group,
        member_id: long_member
      ]

      result = RequestHelpers.extract_common_fields(opts)

      assert result.group_id == long_group
      assert result.member_id == long_member
    end

    test "raises when group_id is missing" do
      opts = [member_id: "member-123"]

      assert_raise KeyError, fn ->
        RequestHelpers.extract_common_fields(opts)
      end
    end

    test "raises when member_id is missing" do
      opts = [group_id: "test-group"]

      assert_raise KeyError, fn ->
        RequestHelpers.extract_common_fields(opts)
      end
    end

    test "ignores extra fields in opts" do
      opts = [
        group_id: "test-group",
        member_id: "member-123",
        extra_field: "ignored",
        another: 42
      ]

      result = RequestHelpers.extract_common_fields(opts)

      assert Enum.sort(Map.keys(result)) == [:group_id, :member_id]
    end
  end
end
