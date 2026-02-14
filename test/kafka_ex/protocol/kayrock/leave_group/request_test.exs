defmodule KafkaEx.Protocol.Kayrock.LeaveGroup.RequestTest do
  use ExUnit.Case, async: true

  # NOTE: RequestHelpers.extract_common_fields/1, build_request_from_template/2,
  # and build_v3_plus_request/2 are tested in request_helpers_test.exs.
  # This file tests protocol dispatch: LeaveGroup.Request.build_request/2

  alias KafkaEx.Protocol.Kayrock.LeaveGroup

  # ---- V0 Request ----

  describe "V0 Request implementation" do
    test "builds request with all required fields" do
      request = %Kayrock.LeaveGroup.V0.Request{}

      opts = [
        group_id: "test-group",
        member_id: "member-123"
      ]

      result = LeaveGroup.Request.build_request(request, opts)

      assert result == %Kayrock.LeaveGroup.V0.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "test-group",
               member_id: "member-123"
             }
    end

    test "builds request with empty strings" do
      request = %Kayrock.LeaveGroup.V0.Request{}

      opts = [
        group_id: "",
        member_id: ""
      ]

      result = LeaveGroup.Request.build_request(request, opts)

      assert result.group_id == ""
      assert result.member_id == ""
    end

    test "builds request with different string values" do
      request = %Kayrock.LeaveGroup.V0.Request{}

      opts = [
        group_id: "consumer-group-1",
        member_id: "consumer-member-abc"
      ]

      result = LeaveGroup.Request.build_request(request, opts)

      assert result == %Kayrock.LeaveGroup.V0.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "consumer-group-1",
               member_id: "consumer-member-abc"
             }
    end

    test "preserves existing correlation_id and client_id if present" do
      request = %Kayrock.LeaveGroup.V0.Request{correlation_id: 42, client_id: "my-client"}

      opts = [
        group_id: "test-group",
        member_id: "test-member"
      ]

      result = LeaveGroup.Request.build_request(request, opts)

      assert result.correlation_id == 42
      assert result.client_id == "my-client"
    end

    test "handles long string values" do
      request = %Kayrock.LeaveGroup.V0.Request{}
      long_group = String.duplicate("a", 500)
      long_member = String.duplicate("b", 500)

      opts = [
        group_id: long_group,
        member_id: long_member
      ]

      result = LeaveGroup.Request.build_request(request, opts)

      assert result.group_id == long_group
      assert result.member_id == long_member
    end
  end

  # ---- V1 Request ----

  describe "V1 Request implementation" do
    test "builds request with all required fields" do
      request = %Kayrock.LeaveGroup.V1.Request{}

      opts = [
        group_id: "test-group",
        member_id: "member-456"
      ]

      result = LeaveGroup.Request.build_request(request, opts)

      assert result == %Kayrock.LeaveGroup.V1.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "test-group",
               member_id: "member-456"
             }
    end

    test "builds request identical to V0" do
      v0_request = %Kayrock.LeaveGroup.V0.Request{}
      v1_request = %Kayrock.LeaveGroup.V1.Request{}

      opts = [
        group_id: "same-group",
        member_id: "same-member"
      ]

      v0_result = LeaveGroup.Request.build_request(v0_request, opts)
      v1_result = LeaveGroup.Request.build_request(v1_request, opts)

      assert v0_result.group_id == v1_result.group_id
      assert v0_result.member_id == v1_result.member_id
    end

    test "preserves correlation_id and client_id" do
      request = %Kayrock.LeaveGroup.V1.Request{correlation_id: 100, client_id: "v1-client"}

      opts = [
        group_id: "test-group",
        member_id: "test-member"
      ]

      result = LeaveGroup.Request.build_request(request, opts)

      assert result.correlation_id == 100
      assert result.client_id == "v1-client"
    end

    test "handles empty strings" do
      request = %Kayrock.LeaveGroup.V1.Request{}

      opts = [
        group_id: "",
        member_id: ""
      ]

      result = LeaveGroup.Request.build_request(request, opts)

      assert result.group_id == ""
      assert result.member_id == ""
    end
  end

  # ---- V2 Request ----

  describe "V2 Request implementation" do
    test "builds request with all required fields (same as V0/V1)" do
      request = %Kayrock.LeaveGroup.V2.Request{}

      opts = [
        group_id: "test-group",
        member_id: "member-789"
      ]

      result = LeaveGroup.Request.build_request(request, opts)

      assert result == %Kayrock.LeaveGroup.V2.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "test-group",
               member_id: "member-789"
             }
    end

    test "V2 request structure matches V1" do
      v1_request = %Kayrock.LeaveGroup.V1.Request{}
      v2_request = %Kayrock.LeaveGroup.V2.Request{}

      opts = [
        group_id: "test-group",
        member_id: "member-123"
      ]

      v1_result = LeaveGroup.Request.build_request(v1_request, opts)
      v2_result = LeaveGroup.Request.build_request(v2_request, opts)

      assert v1_result.group_id == v2_result.group_id
      assert v1_result.member_id == v2_result.member_id
    end

    test "preserves existing correlation_id and client_id" do
      request = %Kayrock.LeaveGroup.V2.Request{correlation_id: 99, client_id: "client-v2"}

      opts = [
        group_id: "test-group",
        member_id: "test-member"
      ]

      result = LeaveGroup.Request.build_request(request, opts)

      assert result.correlation_id == 99
      assert result.client_id == "client-v2"
    end
  end

  # ---- V3 Request (STRUCTURAL CHANGE -- batch leave) ----

  describe "V3 Request implementation (batch leave)" do
    test "builds request with members array" do
      request = %Kayrock.LeaveGroup.V3.Request{}

      opts = [
        group_id: "test-group",
        members: [
          %{member_id: "member-1", group_instance_id: "instance-1"},
          %{member_id: "member-2", group_instance_id: "instance-2"}
        ]
      ]

      result = LeaveGroup.Request.build_request(request, opts)

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

    test "defaults group_instance_id to nil for dynamic membership" do
      request = %Kayrock.LeaveGroup.V3.Request{}

      opts = [
        group_id: "test-group",
        members: [%{member_id: "member-1"}]
      ]

      result = LeaveGroup.Request.build_request(request, opts)

      assert hd(result.members).group_instance_id == nil
    end

    test "handles empty members list" do
      request = %Kayrock.LeaveGroup.V3.Request{}

      opts = [
        group_id: "test-group",
        members: []
      ]

      result = LeaveGroup.Request.build_request(request, opts)

      assert result.members == []
    end

    test "preserves existing correlation_id and client_id" do
      request = %Kayrock.LeaveGroup.V3.Request{correlation_id: 77, client_id: "client-v3"}

      opts = [
        group_id: "test-group",
        members: [%{member_id: "member-1"}]
      ]

      result = LeaveGroup.Request.build_request(request, opts)

      assert result.correlation_id == 77
      assert result.client_id == "client-v3"
    end

    test "does NOT have member_id field (structural change from V0-V2)" do
      request = %Kayrock.LeaveGroup.V3.Request{}

      opts = [
        group_id: "test-group",
        members: [%{member_id: "member-1"}]
      ]

      result = LeaveGroup.Request.build_request(request, opts)

      # V3 struct has no member_id at top level -- only members array
      refute Map.has_key?(result, :member_id)
      assert Map.has_key?(result, :members)
    end
  end

  # ---- V4 Request (FLEX) ----

  describe "V4 Request implementation (FLEX)" do
    test "builds request with members array (same fields as V3)" do
      request = %Kayrock.LeaveGroup.V4.Request{}

      opts = [
        group_id: "test-group",
        members: [
          %{member_id: "member-1", group_instance_id: "static-instance-2"}
        ]
      ]

      result = LeaveGroup.Request.build_request(request, opts)

      assert result.group_id == "test-group"
      assert length(result.members) == 1

      assert hd(result.members) == %{
               member_id: "member-1",
               group_instance_id: "static-instance-2"
             }
    end

    test "defaults group_instance_id to nil" do
      request = %Kayrock.LeaveGroup.V4.Request{}

      opts = [
        group_id: "test-group",
        members: [%{member_id: "member-1"}]
      ]

      result = LeaveGroup.Request.build_request(request, opts)

      assert hd(result.members).group_instance_id == nil
    end

    test "preserves tagged_fields default" do
      request = %Kayrock.LeaveGroup.V4.Request{}

      opts = [
        group_id: "test-group",
        members: [%{member_id: "member-1"}]
      ]

      result = LeaveGroup.Request.build_request(request, opts)

      # tagged_fields is a V4 struct field that we don't set, should remain default
      assert result.tagged_fields == []
    end

    test "preserves existing correlation_id and client_id" do
      request = %Kayrock.LeaveGroup.V4.Request{correlation_id: 88, client_id: "client-v4"}

      opts = [
        group_id: "test-group",
        members: [%{member_id: "member-1"}]
      ]

      result = LeaveGroup.Request.build_request(request, opts)

      assert result.correlation_id == 88
      assert result.client_id == "client-v4"
    end
  end

  # ---- Cross-version consistency ----

  describe "Cross-version consistency (V0-V2)" do
    @base_opts [
      group_id: "cross-version-group",
      member_id: "member-cross"
    ]

    @v0_v2_versions [
      {%Kayrock.LeaveGroup.V0.Request{}, "V0"},
      {%Kayrock.LeaveGroup.V1.Request{}, "V1"},
      {%Kayrock.LeaveGroup.V2.Request{}, "V2"}
    ]

    test "V0-V2 produce identical domain fields" do
      results =
        Enum.map(@v0_v2_versions, fn {template, _label} ->
          LeaveGroup.Request.build_request(template, @base_opts)
        end)

      for result <- results do
        assert result.group_id == "cross-version-group"
        assert result.member_id == "member-cross"
      end
    end
  end

  describe "Cross-version consistency (V3-V4 batch leave)" do
    @batch_opts [
      group_id: "cross-version-group",
      members: [
        %{member_id: "member-1", group_instance_id: "instance-1"},
        %{member_id: "member-2"}
      ]
    ]

    test "V3-V4 produce identical domain fields" do
      v3_result =
        LeaveGroup.Request.build_request(%Kayrock.LeaveGroup.V3.Request{}, @batch_opts)

      v4_result =
        LeaveGroup.Request.build_request(%Kayrock.LeaveGroup.V4.Request{}, @batch_opts)

      for result <- [v3_result, v4_result] do
        assert result.group_id == "cross-version-group"
        assert length(result.members) == 2
        assert Enum.at(result.members, 0).member_id == "member-1"
        assert Enum.at(result.members, 0).group_instance_id == "instance-1"
        assert Enum.at(result.members, 1).member_id == "member-2"
        assert Enum.at(result.members, 1).group_instance_id == nil
      end
    end
  end

  # ---- Any fallback ----

  describe "Any fallback Request implementation" do
    test "routes V0-V2-like map to V0-V2 path (no members key)" do
      template = %{some_field: "v0"}

      opts = [
        group_id: "test-group",
        member_id: "member-123"
      ]

      result = LeaveGroup.Request.build_request(template, opts)

      assert result.group_id == "test-group"
      assert result.member_id == "member-123"
      refute Map.has_key?(result, :members)
    end

    test "routes V3+-like map to V3+ path (has members key)" do
      template = %{members: []}

      opts = [
        group_id: "test-group",
        members: [
          %{member_id: "member-1", group_instance_id: "instance-1"}
        ]
      ]

      result = LeaveGroup.Request.build_request(template, opts)

      assert result.group_id == "test-group"
      assert length(result.members) == 1
      assert hd(result.members).member_id == "member-1"
      assert hd(result.members).group_instance_id == "instance-1"
    end

    test "V3+ path defaults group_instance_id to nil" do
      template = %{members: []}

      opts = [
        group_id: "test-group",
        members: [%{member_id: "member-1"}]
      ]

      result = LeaveGroup.Request.build_request(template, opts)

      assert hd(result.members).group_instance_id == nil
    end

    test "empty map routes to V0-V2 path" do
      template = %{}

      opts = [
        group_id: "test-group",
        member_id: "member-123"
      ]

      result = LeaveGroup.Request.build_request(template, opts)

      assert result.group_id == "test-group"
      assert result.member_id == "member-123"
      refute Map.has_key?(result, :members)
    end

    test "V3+ path with empty members list" do
      template = %{members: []}

      opts = [
        group_id: "test-group",
        members: []
      ]

      result = LeaveGroup.Request.build_request(template, opts)

      assert result.group_id == "test-group"
      assert result.members == []
    end
  end
end
