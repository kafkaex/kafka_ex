defmodule KafkaEx.Protocol.Kayrock.SyncGroup.RequestHelpersTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Protocol.Kayrock.SyncGroup.RequestHelpers

  describe "extract_common_fields/1" do
    test "extracts all required fields" do
      opts = [
        group_id: "my-group",
        generation_id: 5,
        member_id: "member-123",
        group_assignment: [%{member_id: "member-1", member_assignment: <<>>}]
      ]

      result = RequestHelpers.extract_common_fields(opts)

      assert result.group_id == "my-group"
      assert result.generation_id == 5
      assert result.member_id == "member-123"
      assert length(result.group_assignment) == 1
    end

    test "defaults group_assignment to empty list" do
      opts = [group_id: "my-group", generation_id: 1, member_id: "member"]

      result = RequestHelpers.extract_common_fields(opts)

      assert result.group_assignment == []
    end

    test "raises on missing group_id" do
      assert_raise KeyError, fn ->
        RequestHelpers.extract_common_fields(generation_id: 1, member_id: "member")
      end
    end

    test "raises on missing generation_id" do
      assert_raise KeyError, fn ->
        RequestHelpers.extract_common_fields(group_id: "group", member_id: "member")
      end
    end

    test "raises on missing member_id" do
      assert_raise KeyError, fn ->
        RequestHelpers.extract_common_fields(group_id: "group", generation_id: 1)
      end
    end
  end

  describe "build_request_from_template/2" do
    test "builds request with all fields" do
      template = %{}

      opts = [
        group_id: "test-group",
        generation_id: 3,
        member_id: "leader-member",
        group_assignment: [
          %{member_id: "member-1", member_assignment: <<1, 2, 3>>},
          %{member_id: "member-2", member_assignment: <<4, 5, 6>>}
        ]
      ]

      result = RequestHelpers.build_request_from_template(template, opts)

      assert result.group_id == "test-group"
      assert result.generation_id == 3
      assert result.member_id == "leader-member"
      assert length(result.assignments) == 2
    end

    test "builds request with empty group_assignment for non-leaders" do
      template = %{}

      opts = [
        group_id: "test-group",
        generation_id: 3,
        member_id: "follower-member"
      ]

      result = RequestHelpers.build_request_from_template(template, opts)

      assert result.assignments == []
    end
  end
end
