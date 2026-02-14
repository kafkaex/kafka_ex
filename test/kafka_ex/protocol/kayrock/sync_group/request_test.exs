defmodule KafkaEx.Protocol.Kayrock.SyncGroup.RequestTest do
  use ExUnit.Case, async: true

  # NOTE: RequestHelpers.extract_common_fields/1 is tested in request_helpers_test.exs.
  # This file tests protocol dispatch: SyncGroup.Request.build_request/2

  alias KafkaEx.Protocol.Kayrock.SyncGroup

  describe "V0 Request implementation" do
    test "builds request with all required fields" do
      request = %Kayrock.SyncGroup.V0.Request{}

      opts = [
        group_id: "test-group",
        generation_id: 5,
        member_id: "member-123"
      ]

      result = SyncGroup.Request.build_request(request, opts)

      assert result == %Kayrock.SyncGroup.V0.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "test-group",
               generation_id: 5,
               member_id: "member-123",
               assignments: []
             }
    end

    test "builds request with empty strings and zero generation" do
      request = %Kayrock.SyncGroup.V0.Request{}

      opts = [
        group_id: "",
        generation_id: 0,
        member_id: ""
      ]

      result = SyncGroup.Request.build_request(request, opts)

      assert result.group_id == ""
      assert result.generation_id == 0
      assert result.member_id == ""
    end

    test "builds request with group_assignment" do
      request = %Kayrock.SyncGroup.V0.Request{}

      assignments = [
        %{member_id: "member-1", assignment: <<1, 2, 3>>},
        %{member_id: "member-2", assignment: <<4, 5, 6>>}
      ]

      opts = [
        group_id: "consumer-group-1",
        generation_id: 3,
        member_id: "leader-member",
        group_assignment: assignments
      ]

      result = SyncGroup.Request.build_request(request, opts)

      assert result == %Kayrock.SyncGroup.V0.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "consumer-group-1",
               generation_id: 3,
               member_id: "leader-member",
               assignments: assignments
             }
    end

    test "preserves existing correlation_id and client_id if present" do
      request = %Kayrock.SyncGroup.V0.Request{correlation_id: 42, client_id: "my-client"}

      opts = [
        group_id: "test-group",
        generation_id: 1,
        member_id: "test-member"
      ]

      result = SyncGroup.Request.build_request(request, opts)

      assert result.correlation_id == 42
      assert result.client_id == "my-client"
    end

    test "handles unicode characters in group_id and member_id" do
      request = %Kayrock.SyncGroup.V0.Request{}

      opts = [
        group_id: "test-group-rocket",
        generation_id: 10,
        member_id: "member-cafe"
      ]

      result = SyncGroup.Request.build_request(request, opts)

      assert result.group_id == "test-group-rocket"
      assert result.member_id == "member-cafe"
    end

    test "handles long string values" do
      request = %Kayrock.SyncGroup.V0.Request{}
      long_group = String.duplicate("a", 500)
      long_member = String.duplicate("b", 500)

      opts = [
        group_id: long_group,
        generation_id: 100,
        member_id: long_member
      ]

      result = SyncGroup.Request.build_request(request, opts)

      assert result.group_id == long_group
      assert result.member_id == long_member
    end

    test "handles large generation_id" do
      request = %Kayrock.SyncGroup.V0.Request{}

      opts = [
        group_id: "test-group",
        generation_id: 2_147_483_647,
        member_id: "member-123"
      ]

      result = SyncGroup.Request.build_request(request, opts)

      assert result.generation_id == 2_147_483_647
    end
  end

  describe "V1 Request implementation" do
    test "builds request with all required fields" do
      request = %Kayrock.SyncGroup.V1.Request{}

      opts = [
        group_id: "test-group",
        generation_id: 5,
        member_id: "member-123"
      ]

      result = SyncGroup.Request.build_request(request, opts)

      assert result == %Kayrock.SyncGroup.V1.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "test-group",
               generation_id: 5,
               member_id: "member-123",
               assignments: []
             }
    end

    test "builds request with group_assignment" do
      request = %Kayrock.SyncGroup.V1.Request{}

      assignments = [
        %{member_id: "member-1", assignment: <<1, 2, 3>>},
        %{member_id: "member-2", assignment: <<4, 5, 6>>}
      ]

      opts = [
        group_id: "consumer-group-1",
        generation_id: 3,
        member_id: "leader-member",
        group_assignment: assignments
      ]

      result = SyncGroup.Request.build_request(request, opts)

      assert result == %Kayrock.SyncGroup.V1.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "consumer-group-1",
               generation_id: 3,
               member_id: "leader-member",
               assignments: assignments
             }
    end

    test "preserves existing correlation_id and client_id if present" do
      request = %Kayrock.SyncGroup.V1.Request{correlation_id: 42, client_id: "my-client"}

      opts = [
        group_id: "test-group",
        generation_id: 1,
        member_id: "test-member"
      ]

      result = SyncGroup.Request.build_request(request, opts)

      assert result.correlation_id == 42
      assert result.client_id == "my-client"
    end
  end

  describe "V2 Request implementation" do
    test "builds request with all required fields (same as V0/V1)" do
      request = %Kayrock.SyncGroup.V2.Request{}

      opts = [
        group_id: "test-group",
        generation_id: 5,
        member_id: "member-123"
      ]

      result = SyncGroup.Request.build_request(request, opts)

      assert result == %Kayrock.SyncGroup.V2.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "test-group",
               generation_id: 5,
               member_id: "member-123",
               assignments: []
             }
    end

    test "builds request with group_assignment" do
      request = %Kayrock.SyncGroup.V2.Request{}

      assignments = [
        %{member_id: "member-1", assignment: <<1, 2, 3>>}
      ]

      opts = [
        group_id: "consumer-group-v2",
        generation_id: 7,
        member_id: "leader-member",
        group_assignment: assignments
      ]

      result = SyncGroup.Request.build_request(request, opts)

      assert result.group_id == "consumer-group-v2"
      assert result.generation_id == 7
      assert result.member_id == "leader-member"
      assert result.assignments == assignments
    end

    test "V2 request structure matches V1" do
      v1_request = %Kayrock.SyncGroup.V1.Request{}
      v2_request = %Kayrock.SyncGroup.V2.Request{}

      opts = [
        group_id: "test-group",
        generation_id: 5,
        member_id: "member-123"
      ]

      v1_result = SyncGroup.Request.build_request(v1_request, opts)
      v2_result = SyncGroup.Request.build_request(v2_request, opts)

      assert v1_result.group_id == v2_result.group_id
      assert v1_result.generation_id == v2_result.generation_id
      assert v1_result.member_id == v2_result.member_id
      assert v1_result.assignments == v2_result.assignments
    end

    test "preserves existing correlation_id and client_id" do
      request = %Kayrock.SyncGroup.V2.Request{correlation_id: 99, client_id: "client-v2"}

      opts = [
        group_id: "test-group",
        generation_id: 1,
        member_id: "test-member"
      ]

      result = SyncGroup.Request.build_request(request, opts)

      assert result.correlation_id == 99
      assert result.client_id == "client-v2"
    end
  end

  describe "V3 Request implementation" do
    test "builds request with group_instance_id" do
      request = %Kayrock.SyncGroup.V3.Request{}

      opts = [
        group_id: "test-group",
        generation_id: 5,
        member_id: "member-123",
        group_instance_id: "static-instance-1"
      ]

      result = SyncGroup.Request.build_request(request, opts)

      assert result == %Kayrock.SyncGroup.V3.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "test-group",
               generation_id: 5,
               member_id: "member-123",
               group_instance_id: "static-instance-1",
               assignments: []
             }
    end

    test "defaults group_instance_id to nil for dynamic membership" do
      request = %Kayrock.SyncGroup.V3.Request{}

      opts = [
        group_id: "test-group",
        generation_id: 3,
        member_id: ""
      ]

      result = SyncGroup.Request.build_request(request, opts)

      assert result.group_instance_id == nil
    end

    test "builds request with group_assignment and group_instance_id" do
      request = %Kayrock.SyncGroup.V3.Request{}

      assignments = [
        %{member_id: "member-1", assignment: <<1, 2, 3>>}
      ]

      opts = [
        group_id: "consumer-group-v3",
        generation_id: 10,
        member_id: "leader-member",
        group_instance_id: "instance-leader",
        group_assignment: assignments
      ]

      result = SyncGroup.Request.build_request(request, opts)

      assert result.group_id == "consumer-group-v3"
      assert result.generation_id == 10
      assert result.member_id == "leader-member"
      assert result.group_instance_id == "instance-leader"
      assert result.assignments == assignments
    end

    test "preserves existing correlation_id and client_id" do
      request = %Kayrock.SyncGroup.V3.Request{correlation_id: 77, client_id: "client-v3"}

      opts = [
        group_id: "test-group",
        generation_id: 1,
        member_id: "test-member"
      ]

      result = SyncGroup.Request.build_request(request, opts)

      assert result.correlation_id == 77
      assert result.client_id == "client-v3"
    end
  end

  describe "V4 Request implementation (FLEX)" do
    test "builds request with group_instance_id (same fields as V3)" do
      request = %Kayrock.SyncGroup.V4.Request{}

      opts = [
        group_id: "test-group",
        generation_id: 5,
        member_id: "member-123",
        group_instance_id: "static-instance-2"
      ]

      result = SyncGroup.Request.build_request(request, opts)

      assert result.group_id == "test-group"
      assert result.generation_id == 5
      assert result.member_id == "member-123"
      assert result.group_instance_id == "static-instance-2"
      assert result.assignments == []
    end

    test "defaults group_instance_id to nil" do
      request = %Kayrock.SyncGroup.V4.Request{}

      opts = [
        group_id: "test-group",
        generation_id: 1,
        member_id: ""
      ]

      result = SyncGroup.Request.build_request(request, opts)

      assert result.group_instance_id == nil
    end

    test "preserves tagged_fields default" do
      request = %Kayrock.SyncGroup.V4.Request{}

      opts = [
        group_id: "test-group",
        generation_id: 1,
        member_id: "member-123"
      ]

      result = SyncGroup.Request.build_request(request, opts)

      # tagged_fields is a V4 struct field that we don't set, should remain default
      assert result.tagged_fields == []
    end

    test "preserves existing correlation_id and client_id" do
      request = %Kayrock.SyncGroup.V4.Request{correlation_id: 88, client_id: "client-v4"}

      opts = [
        group_id: "test-group",
        generation_id: 1,
        member_id: "test-member"
      ]

      result = SyncGroup.Request.build_request(request, opts)

      assert result.correlation_id == 88
      assert result.client_id == "client-v4"
    end
  end

  describe "Cross-version consistency (V0-V4)" do
    @base_opts [
      group_id: "cross-version-group",
      generation_id: 42,
      member_id: "member-cross"
    ]

    @v0_v2_versions [
      {%Kayrock.SyncGroup.V0.Request{}, "V0"},
      {%Kayrock.SyncGroup.V1.Request{}, "V1"},
      {%Kayrock.SyncGroup.V2.Request{}, "V2"}
    ]

    test "V0-V2 produce identical domain fields" do
      results =
        Enum.map(@v0_v2_versions, fn {template, _label} ->
          SyncGroup.Request.build_request(template, @base_opts)
        end)

      for result <- results do
        assert result.group_id == "cross-version-group"
        assert result.generation_id == 42
        assert result.member_id == "member-cross"
        assert result.assignments == []
      end
    end

    test "V3-V4 produce identical domain fields (with group_instance_id)" do
      v3_v4_opts = @base_opts ++ [group_instance_id: "static-id"]

      v3_result =
        SyncGroup.Request.build_request(%Kayrock.SyncGroup.V3.Request{}, v3_v4_opts)

      v4_result =
        SyncGroup.Request.build_request(%Kayrock.SyncGroup.V4.Request{}, v3_v4_opts)

      for result <- [v3_result, v4_result] do
        assert result.group_id == "cross-version-group"
        assert result.generation_id == 42
        assert result.member_id == "member-cross"
        assert result.group_instance_id == "static-id"
        assert result.assignments == []
      end
    end

    test "V3-V4 without group_instance_id default to nil" do
      v3_result =
        SyncGroup.Request.build_request(%Kayrock.SyncGroup.V3.Request{}, @base_opts)

      v4_result =
        SyncGroup.Request.build_request(%Kayrock.SyncGroup.V4.Request{}, @base_opts)

      assert v3_result.group_instance_id == nil
      assert v4_result.group_instance_id == nil
    end
  end

  describe "V0 Request -- topic_partitions conversion via protocol dispatch" do
    test "converts simple topic_partitions format to Kayrock MemberAssignment" do
      request = %Kayrock.SyncGroup.V0.Request{}

      assignments = [
        %{member_id: "member-1", topic_partitions: [{"topic-a", [0, 1]}, {"topic-b", [2]}]}
      ]

      opts = [
        group_id: "test-group",
        generation_id: 1,
        member_id: "leader",
        group_assignment: assignments
      ]

      result = SyncGroup.Request.build_request(request, opts)

      assert length(result.assignments) == 1
      [converted] = result.assignments
      assert converted.member_id == "member-1"
      assert %Kayrock.MemberAssignment{} = converted.assignment
      assert converted.assignment.version == 0
      assert length(converted.assignment.partition_assignments) == 2

      [pa1, pa2] = converted.assignment.partition_assignments
      assert pa1.topic == "topic-a"
      assert pa1.partitions == [0, 1]
      assert pa2.topic == "topic-b"
      assert pa2.partitions == [2]
    end

    test "passes through already-Kayrock-format assignments unchanged" do
      request = %Kayrock.SyncGroup.V0.Request{}

      kayrock_assignment = %Kayrock.MemberAssignment{
        version: 0,
        partition_assignments: [
          %Kayrock.MemberAssignment.PartitionAssignment{topic: "t1", partitions: [0]}
        ],
        user_data: ""
      }

      assignments = [
        %{member_id: "member-1", assignment: kayrock_assignment}
      ]

      opts = [
        group_id: "test-group",
        generation_id: 1,
        member_id: "leader",
        group_assignment: assignments
      ]

      result = SyncGroup.Request.build_request(request, opts)

      assert length(result.assignments) == 1
      [converted] = result.assignments
      assert converted.member_id == "member-1"
      assert converted.assignment == kayrock_assignment
    end

    test "passes through member_id-only map (no assignment, no topic_partitions)" do
      request = %Kayrock.SyncGroup.V0.Request{}

      assignments = [
        %{member_id: "member-1"}
      ]

      opts = [
        group_id: "test-group",
        generation_id: 1,
        member_id: "leader",
        group_assignment: assignments
      ]

      result = SyncGroup.Request.build_request(request, opts)

      assert length(result.assignments) == 1
      [converted] = result.assignments
      assert converted.member_id == "member-1"
    end
  end

  describe "Any fallback Request implementation" do
    test "routes V0-V2-like struct to V0-V2 path (no group_instance_id)" do
      # A map without group_instance_id
      template = %{some_field: "v0"}

      opts = [
        group_id: "test-group",
        generation_id: 5,
        member_id: "member-123"
      ]

      result = SyncGroup.Request.build_request(template, opts)

      assert result.group_id == "test-group"
      assert result.generation_id == 5
      assert result.member_id == "member-123"
      assert result.assignments == []
      refute Map.has_key?(result, :group_instance_id)
    end

    test "routes V3+-like struct to V3+ path (has group_instance_id)" do
      # A map with group_instance_id
      template = %{group_instance_id: nil}

      opts = [
        group_id: "test-group",
        generation_id: 5,
        member_id: "member-123",
        group_instance_id: "static-1"
      ]

      result = SyncGroup.Request.build_request(template, opts)

      assert result.group_id == "test-group"
      assert result.generation_id == 5
      assert result.member_id == "member-123"
      assert result.group_instance_id == "static-1"
    end

    test "V3+ path defaults group_instance_id to nil" do
      template = %{group_instance_id: nil}

      opts = [
        group_id: "test-group",
        generation_id: 5,
        member_id: "member-123"
      ]

      result = SyncGroup.Request.build_request(template, opts)

      assert result.group_instance_id == nil
    end

    test "handles group_assignment via Any path (V0-V2 branch)" do
      template = %{}

      assignments = [
        %{member_id: "member-1", assignment: <<1, 2, 3>>}
      ]

      opts = [
        group_id: "test-group",
        generation_id: 1,
        member_id: "leader-member",
        group_assignment: assignments
      ]

      result = SyncGroup.Request.build_request(template, opts)

      assert result.assignments == assignments
    end

    test "handles group_assignment via Any path (V3+ branch)" do
      template = %{group_instance_id: nil}

      assignments = [
        %{member_id: "member-1", assignment: <<1, 2, 3>>}
      ]

      opts = [
        group_id: "test-group",
        generation_id: 1,
        member_id: "leader-member",
        group_instance_id: "instance-1",
        group_assignment: assignments
      ]

      result = SyncGroup.Request.build_request(template, opts)

      assert result.assignments == assignments
      assert result.group_instance_id == "instance-1"
    end

    test "empty map routes to V0-V2 path" do
      template = %{}

      opts = [
        group_id: "test-group",
        generation_id: 1,
        member_id: "member-123"
      ]

      result = SyncGroup.Request.build_request(template, opts)

      assert result.group_id == "test-group"
      refute Map.has_key?(result, :group_instance_id)
    end
  end
end
