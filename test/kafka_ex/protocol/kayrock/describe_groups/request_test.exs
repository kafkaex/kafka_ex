defmodule KafkaEx.Protocol.Kayrock.DescribeGroups.RequestTest do
  use ExUnit.Case, async: true

  # NOTE: RequestHelpers.build_request_from_template/2 and build_v3_plus_request/2
  # are tested in request_helpers_test.exs.
  # This file tests protocol dispatch: DescribeGroups.Request.build_request/2

  alias KafkaEx.Protocol.Kayrock.DescribeGroups

  # ---- V0 Request ----

  describe "V0 Request implementation" do
    test "builds request with group names" do
      groups = ["group1", "group2"]

      result =
        DescribeGroups.Request.build_request(%Kayrock.DescribeGroups.V0.Request{},
          group_names: groups
        )

      assert result == %Kayrock.DescribeGroups.V0.Request{groups: groups}
    end

    test "builds request with single group" do
      result =
        DescribeGroups.Request.build_request(%Kayrock.DescribeGroups.V0.Request{},
          group_names: ["single-group"]
        )

      assert result.groups == ["single-group"]
    end

    test "builds request with empty group list" do
      result =
        DescribeGroups.Request.build_request(%Kayrock.DescribeGroups.V0.Request{},
          group_names: []
        )

      assert result.groups == []
    end

    test "preserves existing correlation_id and client_id" do
      request = %Kayrock.DescribeGroups.V0.Request{correlation_id: 42, client_id: "my-client"}

      result = DescribeGroups.Request.build_request(request, group_names: ["test-group"])

      assert result.correlation_id == 42
      assert result.client_id == "my-client"
    end

    test "raises on missing group_names" do
      assert_raise KeyError, fn ->
        DescribeGroups.Request.build_request(%Kayrock.DescribeGroups.V0.Request{}, [])
      end
    end
  end

  # ---- V1 Request ----

  describe "V1 Request implementation" do
    test "builds request with group names" do
      groups = ["group1", "group2"]

      result =
        DescribeGroups.Request.build_request(%Kayrock.DescribeGroups.V1.Request{},
          group_names: groups
        )

      assert result == %Kayrock.DescribeGroups.V1.Request{groups: groups}
    end

    test "preserves correlation_id and client_id" do
      request = %Kayrock.DescribeGroups.V1.Request{correlation_id: 100, client_id: "v1-client"}

      result = DescribeGroups.Request.build_request(request, group_names: ["test-group"])

      assert result.correlation_id == 100
      assert result.client_id == "v1-client"
    end
  end

  # ---- V2 Request ----

  describe "V2 Request implementation" do
    test "builds request with group names (schema-identical to V0/V1)" do
      groups = ["group1", "group2"]

      result =
        DescribeGroups.Request.build_request(%Kayrock.DescribeGroups.V2.Request{},
          group_names: groups
        )

      assert result == %Kayrock.DescribeGroups.V2.Request{groups: groups}
    end

    test "preserves correlation_id and client_id" do
      request = %Kayrock.DescribeGroups.V2.Request{correlation_id: 99, client_id: "v2-client"}

      result = DescribeGroups.Request.build_request(request, group_names: ["test-group"])

      assert result.correlation_id == 99
      assert result.client_id == "v2-client"
    end
  end

  # ---- V3 Request ----

  describe "V3 Request implementation" do
    test "builds request with include_authorized_operations" do
      groups = ["group1", "group2"]

      result =
        DescribeGroups.Request.build_request(%Kayrock.DescribeGroups.V3.Request{},
          group_names: groups,
          include_authorized_operations: true
        )

      assert result == %Kayrock.DescribeGroups.V3.Request{
               groups: groups,
               include_authorized_operations: true
             }
    end

    test "defaults include_authorized_operations to false" do
      result =
        DescribeGroups.Request.build_request(%Kayrock.DescribeGroups.V3.Request{},
          group_names: ["test-group"]
        )

      assert result.include_authorized_operations == false
    end

    test "preserves correlation_id and client_id" do
      request = %Kayrock.DescribeGroups.V3.Request{correlation_id: 77, client_id: "v3-client"}

      result = DescribeGroups.Request.build_request(request, group_names: ["test-group"])

      assert result.correlation_id == 77
      assert result.client_id == "v3-client"
    end
  end

  # ---- V4 Request ----

  describe "V4 Request implementation" do
    test "builds request with include_authorized_operations (same fields as V3)" do
      groups = ["group1"]

      result =
        DescribeGroups.Request.build_request(%Kayrock.DescribeGroups.V4.Request{},
          group_names: groups,
          include_authorized_operations: true
        )

      assert result.groups == groups
      assert result.include_authorized_operations == true
    end

    test "defaults include_authorized_operations to false" do
      result =
        DescribeGroups.Request.build_request(%Kayrock.DescribeGroups.V4.Request{},
          group_names: ["test-group"]
        )

      assert result.include_authorized_operations == false
    end

    test "preserves correlation_id and client_id" do
      request = %Kayrock.DescribeGroups.V4.Request{correlation_id: 88, client_id: "v4-client"}

      result = DescribeGroups.Request.build_request(request, group_names: ["test-group"])

      assert result.correlation_id == 88
      assert result.client_id == "v4-client"
    end
  end

  # ---- V5 Request (FLEX) ----

  describe "V5 Request implementation (FLEX)" do
    test "builds request with include_authorized_operations (same fields as V3/V4)" do
      groups = ["group1", "group2"]

      result =
        DescribeGroups.Request.build_request(%Kayrock.DescribeGroups.V5.Request{},
          group_names: groups,
          include_authorized_operations: true
        )

      assert result.groups == groups
      assert result.include_authorized_operations == true
    end

    test "defaults include_authorized_operations to false" do
      result =
        DescribeGroups.Request.build_request(%Kayrock.DescribeGroups.V5.Request{},
          group_names: ["test-group"]
        )

      assert result.include_authorized_operations == false
    end

    test "preserves tagged_fields default" do
      result =
        DescribeGroups.Request.build_request(%Kayrock.DescribeGroups.V5.Request{},
          group_names: ["test-group"]
        )

      assert result.tagged_fields == []
    end

    test "preserves correlation_id and client_id" do
      request = %Kayrock.DescribeGroups.V5.Request{correlation_id: 55, client_id: "v5-client"}

      result = DescribeGroups.Request.build_request(request, group_names: ["test-group"])

      assert result.correlation_id == 55
      assert result.client_id == "v5-client"
    end
  end

  # ---- Cross-version consistency ----

  describe "Cross-version consistency (V0-V5)" do
    @base_opts [group_names: ["cross-version-group-1", "cross-version-group-2"]]

    @v0_v2_versions [
      {%Kayrock.DescribeGroups.V0.Request{}, "V0"},
      {%Kayrock.DescribeGroups.V1.Request{}, "V1"},
      {%Kayrock.DescribeGroups.V2.Request{}, "V2"}
    ]

    test "V0-V2 produce identical domain fields" do
      results =
        Enum.map(@v0_v2_versions, fn {template, _label} ->
          DescribeGroups.Request.build_request(template, @base_opts)
        end)

      for result <- results do
        assert result.groups == ["cross-version-group-1", "cross-version-group-2"]
      end
    end

    test "V3-V5 produce identical domain fields (with include_authorized_operations)" do
      v3_v5_opts = @base_opts ++ [include_authorized_operations: true]

      v3_result =
        DescribeGroups.Request.build_request(%Kayrock.DescribeGroups.V3.Request{}, v3_v5_opts)

      v4_result =
        DescribeGroups.Request.build_request(%Kayrock.DescribeGroups.V4.Request{}, v3_v5_opts)

      v5_result =
        DescribeGroups.Request.build_request(%Kayrock.DescribeGroups.V5.Request{}, v3_v5_opts)

      for result <- [v3_result, v4_result, v5_result] do
        assert result.groups == ["cross-version-group-1", "cross-version-group-2"]
        assert result.include_authorized_operations == true
      end
    end

    test "V3-V5 without include_authorized_operations default to false" do
      v3_result =
        DescribeGroups.Request.build_request(%Kayrock.DescribeGroups.V3.Request{}, @base_opts)

      v4_result =
        DescribeGroups.Request.build_request(%Kayrock.DescribeGroups.V4.Request{}, @base_opts)

      v5_result =
        DescribeGroups.Request.build_request(%Kayrock.DescribeGroups.V5.Request{}, @base_opts)

      assert v3_result.include_authorized_operations == false
      assert v4_result.include_authorized_operations == false
      assert v5_result.include_authorized_operations == false
    end
  end

  # ---- Any fallback ----

  describe "Any fallback Request implementation" do
    test "routes V0-V2-like map to V0-V2 path (no include_authorized_operations)" do
      template = %{some_field: "v0"}

      result = DescribeGroups.Request.build_request(template, group_names: ["test-group"])

      assert result.groups == ["test-group"]
      refute Map.has_key?(result, :include_authorized_operations)
    end

    test "routes V3+-like map to V3+ path (has include_authorized_operations)" do
      template = %{include_authorized_operations: nil}

      result =
        DescribeGroups.Request.build_request(template,
          group_names: ["test-group"],
          include_authorized_operations: true
        )

      assert result.groups == ["test-group"]
      assert result.include_authorized_operations == true
    end

    test "V3+ path defaults include_authorized_operations to false" do
      template = %{include_authorized_operations: nil}

      result = DescribeGroups.Request.build_request(template, group_names: ["test-group"])

      assert result.include_authorized_operations == false
    end

    test "empty map routes to V0-V2 path" do
      template = %{}

      result = DescribeGroups.Request.build_request(template, group_names: ["test-group"])

      assert result.groups == ["test-group"]
      refute Map.has_key?(result, :include_authorized_operations)
    end
  end
end
