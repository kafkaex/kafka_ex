defmodule KafkaEx.Protocol.Kayrock.LeaveGroup.RequestHelpersTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Protocol.Kayrock.LeaveGroup.RequestHelpers

  describe "extract_common_fields/1" do
    test "extracts group_id and member_id" do
      opts = [group_id: "my-consumer-group", member_id: "member-123"]

      result = RequestHelpers.extract_common_fields(opts)

      assert result.group_id == "my-consumer-group"
      assert result.member_id == "member-123"
    end

    test "raises on missing group_id" do
      assert_raise KeyError, fn ->
        RequestHelpers.extract_common_fields(member_id: "member")
      end
    end

    test "raises on missing member_id" do
      assert_raise KeyError, fn ->
        RequestHelpers.extract_common_fields(group_id: "group")
      end
    end

    test "raises with empty options" do
      assert_raise KeyError, fn ->
        RequestHelpers.extract_common_fields([])
      end
    end
  end
end
