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

    test "preserves existing template fields" do
      template = %{correlation_id: 42, client_id: "my-client"}

      opts = [
        group_id: "test-group",
        generation_id: 1,
        member_id: "member-123"
      ]

      result = RequestHelpers.build_request_from_template(template, opts)

      assert result.correlation_id == 42
      assert result.client_id == "my-client"
      assert result.group_id == "test-group"
    end
  end

  describe "build_v3_plus_request/2" do
    test "builds request with group_instance_id" do
      template = %{group_instance_id: nil}

      opts = [
        group_id: "test-group",
        generation_id: 5,
        member_id: "member-123",
        group_instance_id: "static-instance-1"
      ]

      result = RequestHelpers.build_v3_plus_request(template, opts)

      assert result.group_id == "test-group"
      assert result.generation_id == 5
      assert result.member_id == "member-123"
      assert result.group_instance_id == "static-instance-1"
      assert result.assignments == []
    end

    test "defaults group_instance_id to nil when not provided" do
      template = %{group_instance_id: nil}

      opts = [
        group_id: "test-group",
        generation_id: 3,
        member_id: "member-123"
      ]

      result = RequestHelpers.build_v3_plus_request(template, opts)

      assert result.group_instance_id == nil
    end

    test "includes group_assignment alongside group_instance_id" do
      template = %{group_instance_id: nil}

      assignments = [
        %{member_id: "member-1", assignment: <<1, 2, 3>>}
      ]

      opts = [
        group_id: "test-group",
        generation_id: 10,
        member_id: "leader-member",
        group_instance_id: "instance-leader",
        group_assignment: assignments
      ]

      result = RequestHelpers.build_v3_plus_request(template, opts)

      assert result.group_id == "test-group"
      assert result.generation_id == 10
      assert result.member_id == "leader-member"
      assert result.group_instance_id == "instance-leader"
      assert result.assignments == assignments
    end

    test "preserves existing template fields" do
      template = %{group_instance_id: nil, correlation_id: 77, client_id: "client-v3"}

      opts = [
        group_id: "test-group",
        generation_id: 1,
        member_id: "member-123",
        group_instance_id: "static-1"
      ]

      result = RequestHelpers.build_v3_plus_request(template, opts)

      assert result.correlation_id == 77
      assert result.client_id == "client-v3"
      assert result.group_instance_id == "static-1"
    end

    test "overrides group_instance_id from template with opts value" do
      template = %{group_instance_id: "old-instance"}

      opts = [
        group_id: "test-group",
        generation_id: 1,
        member_id: "member-123",
        group_instance_id: "new-instance"
      ]

      result = RequestHelpers.build_v3_plus_request(template, opts)

      assert result.group_instance_id == "new-instance"
    end
  end

  # ---- convert_group_assignment (private, tested via build_request_from_template) ----

  describe "convert_group_assignment via build_request_from_template/2" do
    test "converts topic_partitions format to Kayrock MemberAssignment structs" do
      template = %{}

      opts = [
        group_id: "test-group",
        generation_id: 1,
        member_id: "leader",
        group_assignment: [
          %{member_id: "member-1", topic_partitions: [{"topic-a", [0, 1]}, {"topic-b", [2]}]},
          %{member_id: "member-2", topic_partitions: [{"topic-a", [3]}]}
        ]
      ]

      result = RequestHelpers.build_request_from_template(template, opts)

      assert length(result.assignments) == 2

      [m1, m2] = result.assignments

      # member-1
      assert m1.member_id == "member-1"
      assert %Kayrock.MemberAssignment{} = m1.assignment
      assert m1.assignment.version == 0
      assert m1.assignment.user_data == ""
      assert length(m1.assignment.partition_assignments) == 2

      [pa1, pa2] = m1.assignment.partition_assignments
      assert %Kayrock.MemberAssignment.PartitionAssignment{topic: "topic-a", partitions: [0, 1]} = pa1
      assert %Kayrock.MemberAssignment.PartitionAssignment{topic: "topic-b", partitions: [2]} = pa2

      # member-2
      assert m2.member_id == "member-2"
      assert %Kayrock.MemberAssignment{} = m2.assignment
      assert length(m2.assignment.partition_assignments) == 1
    end

    test "passes through already-Kayrock MemberAssignment format unchanged" do
      kayrock_assignment = %Kayrock.MemberAssignment{
        version: 0,
        partition_assignments: [
          %Kayrock.MemberAssignment.PartitionAssignment{topic: "t1", partitions: [0]}
        ],
        user_data: "custom"
      }

      template = %{}

      opts = [
        group_id: "test-group",
        generation_id: 1,
        member_id: "leader",
        group_assignment: [
          %{member_id: "member-1", assignment: kayrock_assignment}
        ]
      ]

      result = RequestHelpers.build_request_from_template(template, opts)

      assert length(result.assignments) == 1
      [converted] = result.assignments
      assert converted.member_id == "member-1"
      assert converted.assignment == kayrock_assignment
      # user_data is preserved, proving passthrough (not re-creation)
      assert converted.assignment.user_data == "custom"
    end

    test "passes through member_id-only map (no assignment, no topic_partitions)" do
      template = %{}

      opts = [
        group_id: "test-group",
        generation_id: 1,
        member_id: "leader",
        group_assignment: [
          %{member_id: "member-1"}
        ]
      ]

      result = RequestHelpers.build_request_from_template(template, opts)

      assert length(result.assignments) == 1
      [converted] = result.assignments
      assert converted.member_id == "member-1"
      # No assignment key set -- this is the edge case passthrough
      refute Map.has_key?(converted, :assignment)
    end

    test "handles empty topic_partitions list" do
      template = %{}

      opts = [
        group_id: "test-group",
        generation_id: 1,
        member_id: "leader",
        group_assignment: [
          %{member_id: "member-1", topic_partitions: []}
        ]
      ]

      result = RequestHelpers.build_request_from_template(template, opts)

      assert length(result.assignments) == 1
      [converted] = result.assignments
      assert converted.member_id == "member-1"
      assert %Kayrock.MemberAssignment{} = converted.assignment
      assert converted.assignment.partition_assignments == []
    end

    test "handles empty group_assignment list" do
      template = %{}

      opts = [
        group_id: "test-group",
        generation_id: 1,
        member_id: "follower"
      ]

      result = RequestHelpers.build_request_from_template(template, opts)

      assert result.assignments == []
    end

    test "handles mixed assignment formats in same list" do
      kayrock_assignment = %Kayrock.MemberAssignment{
        version: 0,
        partition_assignments: [
          %Kayrock.MemberAssignment.PartitionAssignment{topic: "t1", partitions: [0]}
        ],
        user_data: ""
      }

      template = %{}

      opts = [
        group_id: "test-group",
        generation_id: 1,
        member_id: "leader",
        group_assignment: [
          # Kayrock format passthrough
          %{member_id: "member-1", assignment: kayrock_assignment},
          # Simple format conversion
          %{member_id: "member-2", topic_partitions: [{"t2", [1, 2]}]},
          # Edge case passthrough
          %{member_id: "member-3"}
        ]
      ]

      result = RequestHelpers.build_request_from_template(template, opts)

      assert length(result.assignments) == 3

      [m1, m2, m3] = result.assignments

      # Kayrock passthrough
      assert m1.assignment == kayrock_assignment

      # Converted from topic_partitions
      assert %Kayrock.MemberAssignment{} = m2.assignment
      assert length(m2.assignment.partition_assignments) == 1

      # Edge case passthrough -- no assignment key
      assert m3.member_id == "member-3"
    end
  end

  # ---- extract_common_fields edge cases ----

  describe "extract_common_fields/1 (edge cases)" do
    test "handles empty string group_id" do
      opts = [group_id: "", generation_id: 0, member_id: ""]

      result = RequestHelpers.extract_common_fields(opts)

      assert result.group_id == ""
      assert result.generation_id == 0
      assert result.member_id == ""
    end

    test "handles negative generation_id" do
      opts = [group_id: "group", generation_id: -1, member_id: "member"]

      result = RequestHelpers.extract_common_fields(opts)

      assert result.generation_id == -1
    end

    test "handles large generation_id (int32 max)" do
      opts = [group_id: "group", generation_id: 2_147_483_647, member_id: "member"]

      result = RequestHelpers.extract_common_fields(opts)

      assert result.generation_id == 2_147_483_647
    end

    test "ignores extra opts keys" do
      opts = [
        group_id: "group",
        generation_id: 1,
        member_id: "member",
        extra_key: "ignored",
        another: 42
      ]

      result = RequestHelpers.extract_common_fields(opts)

      assert Enum.sort(Map.keys(result)) == [
               :generation_id,
               :group_assignment,
               :group_id,
               :member_id
             ]
    end
  end
end
