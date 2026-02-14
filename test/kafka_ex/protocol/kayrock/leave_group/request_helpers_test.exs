defmodule KafkaEx.Protocol.Kayrock.LeaveGroup.RequestHelpersTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Protocol.Kayrock.LeaveGroup.RequestHelpers

  # ---- extract_common_fields/1 ----

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

    test "ignores extra fields" do
      opts = [
        group_id: "g",
        member_id: "m",
        extra: "ignored"
      ]

      result = RequestHelpers.extract_common_fields(opts)

      assert Enum.sort(Map.keys(result)) == [:group_id, :member_id]
    end
  end

  # ---- build_request_from_template/2 ----

  describe "build_request_from_template/2" do
    test "populates template with common fields" do
      template = %{group_id: nil, member_id: nil}

      opts = [
        group_id: "test-group",
        member_id: "member-123"
      ]

      result = RequestHelpers.build_request_from_template(template, opts)

      assert result.group_id == "test-group"
      assert result.member_id == "member-123"
    end

    test "preserves other fields in template" do
      template = %{
        group_id: nil,
        member_id: nil,
        correlation_id: 42,
        client_id: "my-client"
      }

      opts = [
        group_id: "test-group",
        member_id: "member-123"
      ]

      result = RequestHelpers.build_request_from_template(template, opts)

      assert result.correlation_id == 42
      assert result.client_id == "my-client"
    end

    test "handles empty strings" do
      template = %{group_id: nil, member_id: nil}

      opts = [
        group_id: "",
        member_id: ""
      ]

      result = RequestHelpers.build_request_from_template(template, opts)

      assert result.group_id == ""
      assert result.member_id == ""
    end

    test "handles long strings" do
      template = %{group_id: nil, member_id: nil}
      long_group = String.duplicate("a", 1000)
      long_member = String.duplicate("b", 1000)

      opts = [
        group_id: long_group,
        member_id: long_member
      ]

      result = RequestHelpers.build_request_from_template(template, opts)

      assert result.group_id == long_group
      assert result.member_id == long_member
    end

    test "raises when required fields missing" do
      template = %{group_id: nil, member_id: nil}

      assert_raise KeyError, fn ->
        RequestHelpers.build_request_from_template(template, [])
      end
    end

    test "works with plain map template" do
      template = %{}

      opts = [
        group_id: "test-group",
        member_id: "member-123"
      ]

      result = RequestHelpers.build_request_from_template(template, opts)

      assert result.group_id == "test-group"
      assert result.member_id == "member-123"
    end
  end

  # ---- build_v3_plus_request/2 ----

  describe "build_v3_plus_request/2" do
    test "populates group_id and members array" do
      template = %{group_id: nil, members: []}

      opts = [
        group_id: "test-group",
        members: [
          %{member_id: "member-1", group_instance_id: "instance-1"},
          %{member_id: "member-2", group_instance_id: "instance-2"}
        ]
      ]

      result = RequestHelpers.build_v3_plus_request(template, opts)

      assert result.group_id == "test-group"
      assert length(result.members) == 2

      assert Enum.at(result.members, 0) == %{
               member_id: "member-1",
               group_instance_id: "instance-1"
             }

      assert Enum.at(result.members, 1) == %{
               member_id: "member-2",
               group_instance_id: "instance-2"
             }
    end

    test "defaults group_instance_id to nil when not provided in member" do
      template = %{group_id: nil, members: []}

      opts = [
        group_id: "test-group",
        members: [
          %{member_id: "member-1"}
        ]
      ]

      result = RequestHelpers.build_v3_plus_request(template, opts)

      assert Enum.at(result.members, 0) == %{
               member_id: "member-1",
               group_instance_id: nil
             }
    end

    test "handles empty members list" do
      template = %{group_id: nil, members: []}

      opts = [
        group_id: "test-group",
        members: []
      ]

      result = RequestHelpers.build_v3_plus_request(template, opts)

      assert result.group_id == "test-group"
      assert result.members == []
    end

    test "handles single member" do
      template = %{group_id: nil, members: []}

      opts = [
        group_id: "test-group",
        members: [
          %{member_id: "sole-member", group_instance_id: "sole-instance"}
        ]
      ]

      result = RequestHelpers.build_v3_plus_request(template, opts)

      assert length(result.members) == 1

      assert hd(result.members) == %{
               member_id: "sole-member",
               group_instance_id: "sole-instance"
             }
    end

    test "handles mixed members (some with group_instance_id, some without)" do
      template = %{group_id: nil, members: []}

      opts = [
        group_id: "test-group",
        members: [
          %{member_id: "static-member", group_instance_id: "instance-1"},
          %{member_id: "dynamic-member"}
        ]
      ]

      result = RequestHelpers.build_v3_plus_request(template, opts)

      assert Enum.at(result.members, 0).group_instance_id == "instance-1"
      assert Enum.at(result.members, 1).group_instance_id == nil
    end

    test "preserves other fields in template" do
      template = %{
        group_id: nil,
        members: [],
        correlation_id: 99,
        client_id: "v3-client"
      }

      opts = [
        group_id: "test-group",
        members: [%{member_id: "member-1"}]
      ]

      result = RequestHelpers.build_v3_plus_request(template, opts)

      assert result.correlation_id == 99
      assert result.client_id == "v3-client"
    end

    test "raises when group_id is missing" do
      template = %{group_id: nil, members: []}

      assert_raise KeyError, fn ->
        RequestHelpers.build_v3_plus_request(template, members: [%{member_id: "m"}])
      end
    end

    test "raises when members is missing" do
      template = %{group_id: nil, members: []}

      assert_raise KeyError, fn ->
        RequestHelpers.build_v3_plus_request(template, group_id: "group")
      end
    end

    test "raises when member_id is missing from a member map" do
      template = %{group_id: nil, members: []}

      assert_raise KeyError, fn ->
        RequestHelpers.build_v3_plus_request(template,
          group_id: "group",
          members: [%{group_instance_id: "instance-no-member"}]
        )
      end
    end

    test "works with plain map template" do
      template = %{}

      opts = [
        group_id: "test-group",
        members: [%{member_id: "member-1", group_instance_id: "inst-1"}]
      ]

      result = RequestHelpers.build_v3_plus_request(template, opts)

      assert result.group_id == "test-group"
      assert length(result.members) == 1
    end

    test "handles many members" do
      template = %{group_id: nil, members: []}

      members =
        Enum.map(1..50, fn i ->
          %{member_id: "member-#{i}", group_instance_id: "instance-#{i}"}
        end)

      opts = [group_id: "large-group", members: members]

      result = RequestHelpers.build_v3_plus_request(template, opts)

      assert length(result.members) == 50
      assert Enum.at(result.members, 0).member_id == "member-1"
      assert Enum.at(result.members, 49).member_id == "member-50"
    end
  end
end
