defmodule KafkaEx.Protocol.Kayrock.Heartbeat.RequestHelpersTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Protocol.Kayrock.Heartbeat.RequestHelpers

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
  end
end
