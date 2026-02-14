defmodule KafkaEx.Protocol.Kayrock.Heartbeat.RequestTest do
  use ExUnit.Case, async: true

  # NOTE: RequestHelpers.extract_common_fields/1, build_request_from_template/2,
  # and build_v3_plus_request/2 are tested in request_helpers_test.exs.
  # This file tests protocol dispatch: Heartbeat.Request.build_request/2

  alias KafkaEx.Protocol.Kayrock.Heartbeat

  # ---- V0 Request ----

  describe "V0 Request implementation" do
    test "builds request with all required fields" do
      request = %Kayrock.Heartbeat.V0.Request{}

      opts = [
        group_id: "test-group",
        member_id: "member-123",
        generation_id: 5
      ]

      result = Heartbeat.Request.build_request(request, opts)

      assert result == %Kayrock.Heartbeat.V0.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "test-group",
               member_id: "member-123",
               generation_id: 5
             }
    end

    test "builds request with generation_id zero" do
      request = %Kayrock.Heartbeat.V0.Request{}

      opts = [
        group_id: "new-group",
        member_id: "new-member",
        generation_id: 0
      ]

      result = Heartbeat.Request.build_request(request, opts)

      assert result.generation_id == 0
    end

    test "builds request with different string values" do
      request = %Kayrock.Heartbeat.V0.Request{}

      opts = [
        group_id: "consumer-group-1",
        member_id: "consumer-member-abc",
        generation_id: 100
      ]

      result = Heartbeat.Request.build_request(request, opts)

      assert result == %Kayrock.Heartbeat.V0.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "consumer-group-1",
               member_id: "consumer-member-abc",
               generation_id: 100
             }
    end

    test "preserves existing correlation_id and client_id if present" do
      request = %Kayrock.Heartbeat.V0.Request{correlation_id: 42, client_id: "my-client"}

      opts = [
        group_id: "test-group",
        member_id: "test-member",
        generation_id: 10
      ]

      result = Heartbeat.Request.build_request(request, opts)

      assert result.correlation_id == 42
      assert result.client_id == "my-client"
    end

    test "handles empty strings for group_id and member_id" do
      request = %Kayrock.Heartbeat.V0.Request{}

      opts = [
        group_id: "",
        member_id: "",
        generation_id: 1
      ]

      result = Heartbeat.Request.build_request(request, opts)

      assert result.group_id == ""
      assert result.member_id == ""
    end

    test "handles large generation_id values" do
      request = %Kayrock.Heartbeat.V0.Request{}

      opts = [
        group_id: "test-group",
        member_id: "test-member",
        generation_id: 2_147_483_647
      ]

      result = Heartbeat.Request.build_request(request, opts)

      assert result.generation_id == 2_147_483_647
    end
  end

  # ---- V1 Request ----

  describe "V1 Request implementation" do
    test "builds request with all required fields" do
      request = %Kayrock.Heartbeat.V1.Request{}

      opts = [
        group_id: "test-group",
        member_id: "member-456",
        generation_id: 10
      ]

      result = Heartbeat.Request.build_request(request, opts)

      assert result == %Kayrock.Heartbeat.V1.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "test-group",
               member_id: "member-456",
               generation_id: 10
             }
    end

    test "builds request identical to V0" do
      v0_request = %Kayrock.Heartbeat.V0.Request{}
      v1_request = %Kayrock.Heartbeat.V1.Request{}

      opts = [
        group_id: "same-group",
        member_id: "same-member",
        generation_id: 7
      ]

      v0_result = Heartbeat.Request.build_request(v0_request, opts)
      v1_result = Heartbeat.Request.build_request(v1_request, opts)

      # Both should have same field values (struct names differ)
      assert v0_result.group_id == v1_result.group_id
      assert v0_result.member_id == v1_result.member_id
      assert v0_result.generation_id == v1_result.generation_id
    end

    test "preserves correlation_id and client_id" do
      request = %Kayrock.Heartbeat.V1.Request{correlation_id: 100, client_id: "v1-client"}

      opts = [
        group_id: "test-group",
        member_id: "test-member",
        generation_id: 5
      ]

      result = Heartbeat.Request.build_request(request, opts)

      assert result.correlation_id == 100
      assert result.client_id == "v1-client"
    end
  end

  # ---- V2 Request ----

  describe "V2 Request implementation" do
    test "builds request with all required fields (same as V0/V1)" do
      request = %Kayrock.Heartbeat.V2.Request{}

      opts = [
        group_id: "test-group",
        member_id: "member-789",
        generation_id: 15
      ]

      result = Heartbeat.Request.build_request(request, opts)

      assert result == %Kayrock.Heartbeat.V2.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "test-group",
               member_id: "member-789",
               generation_id: 15
             }
    end

    test "V2 request structure matches V1" do
      v1_request = %Kayrock.Heartbeat.V1.Request{}
      v2_request = %Kayrock.Heartbeat.V2.Request{}

      opts = [
        group_id: "test-group",
        member_id: "member-123",
        generation_id: 5
      ]

      v1_result = Heartbeat.Request.build_request(v1_request, opts)
      v2_result = Heartbeat.Request.build_request(v2_request, opts)

      assert v1_result.group_id == v2_result.group_id
      assert v1_result.member_id == v2_result.member_id
      assert v1_result.generation_id == v2_result.generation_id
    end

    test "preserves existing correlation_id and client_id" do
      request = %Kayrock.Heartbeat.V2.Request{correlation_id: 99, client_id: "client-v2"}

      opts = [
        group_id: "test-group",
        member_id: "test-member",
        generation_id: 1
      ]

      result = Heartbeat.Request.build_request(request, opts)

      assert result.correlation_id == 99
      assert result.client_id == "client-v2"
    end
  end

  # ---- V3 Request ----

  describe "V3 Request implementation" do
    test "builds request with group_instance_id" do
      request = %Kayrock.Heartbeat.V3.Request{}

      opts = [
        group_id: "test-group",
        member_id: "member-123",
        generation_id: 5,
        group_instance_id: "static-instance-1"
      ]

      result = Heartbeat.Request.build_request(request, opts)

      assert result == %Kayrock.Heartbeat.V3.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "test-group",
               member_id: "member-123",
               generation_id: 5,
               group_instance_id: "static-instance-1"
             }
    end

    test "defaults group_instance_id to nil for dynamic membership" do
      request = %Kayrock.Heartbeat.V3.Request{}

      opts = [
        group_id: "test-group",
        member_id: "member-123",
        generation_id: 3
      ]

      result = Heartbeat.Request.build_request(request, opts)

      assert result.group_instance_id == nil
    end

    test "preserves existing correlation_id and client_id" do
      request = %Kayrock.Heartbeat.V3.Request{correlation_id: 77, client_id: "client-v3"}

      opts = [
        group_id: "test-group",
        member_id: "test-member",
        generation_id: 1
      ]

      result = Heartbeat.Request.build_request(request, opts)

      assert result.correlation_id == 77
      assert result.client_id == "client-v3"
    end
  end

  # ---- V4 Request (FLEX) ----

  describe "V4 Request implementation (FLEX)" do
    test "builds request with group_instance_id (same fields as V3)" do
      request = %Kayrock.Heartbeat.V4.Request{}

      opts = [
        group_id: "test-group",
        member_id: "member-123",
        generation_id: 5,
        group_instance_id: "static-instance-2"
      ]

      result = Heartbeat.Request.build_request(request, opts)

      assert result.group_id == "test-group"
      assert result.generation_id == 5
      assert result.member_id == "member-123"
      assert result.group_instance_id == "static-instance-2"
    end

    test "defaults group_instance_id to nil" do
      request = %Kayrock.Heartbeat.V4.Request{}

      opts = [
        group_id: "test-group",
        member_id: "member-123",
        generation_id: 1
      ]

      result = Heartbeat.Request.build_request(request, opts)

      assert result.group_instance_id == nil
    end

    test "preserves tagged_fields default" do
      request = %Kayrock.Heartbeat.V4.Request{}

      opts = [
        group_id: "test-group",
        member_id: "member-123",
        generation_id: 1
      ]

      result = Heartbeat.Request.build_request(request, opts)

      # tagged_fields is a V4 struct field that we don't set, should remain default
      assert result.tagged_fields == []
    end

    test "preserves existing correlation_id and client_id" do
      request = %Kayrock.Heartbeat.V4.Request{correlation_id: 88, client_id: "client-v4"}

      opts = [
        group_id: "test-group",
        member_id: "test-member",
        generation_id: 1
      ]

      result = Heartbeat.Request.build_request(request, opts)

      assert result.correlation_id == 88
      assert result.client_id == "client-v4"
    end
  end

  # ---- Cross-version consistency ----

  describe "Cross-version consistency (V0-V4)" do
    @base_opts [
      group_id: "cross-version-group",
      member_id: "member-cross",
      generation_id: 42
    ]

    @v0_v2_versions [
      {%Kayrock.Heartbeat.V0.Request{}, "V0"},
      {%Kayrock.Heartbeat.V1.Request{}, "V1"},
      {%Kayrock.Heartbeat.V2.Request{}, "V2"}
    ]

    test "V0-V2 produce identical domain fields" do
      results =
        Enum.map(@v0_v2_versions, fn {template, _label} ->
          Heartbeat.Request.build_request(template, @base_opts)
        end)

      for result <- results do
        assert result.group_id == "cross-version-group"
        assert result.member_id == "member-cross"
        assert result.generation_id == 42
      end
    end

    test "V3-V4 produce identical domain fields (with group_instance_id)" do
      v3_v4_opts = @base_opts ++ [group_instance_id: "static-id"]

      v3_result =
        Heartbeat.Request.build_request(%Kayrock.Heartbeat.V3.Request{}, v3_v4_opts)

      v4_result =
        Heartbeat.Request.build_request(%Kayrock.Heartbeat.V4.Request{}, v3_v4_opts)

      for result <- [v3_result, v4_result] do
        assert result.group_id == "cross-version-group"
        assert result.member_id == "member-cross"
        assert result.generation_id == 42
        assert result.group_instance_id == "static-id"
      end
    end

    test "V3-V4 without group_instance_id default to nil" do
      v3_result =
        Heartbeat.Request.build_request(%Kayrock.Heartbeat.V3.Request{}, @base_opts)

      v4_result =
        Heartbeat.Request.build_request(%Kayrock.Heartbeat.V4.Request{}, @base_opts)

      assert v3_result.group_instance_id == nil
      assert v4_result.group_instance_id == nil
    end
  end

  # ---- Any fallback ----

  describe "Any fallback Request implementation" do
    test "routes V0-V2-like map to V0-V2 path (no group_instance_id)" do
      template = %{some_field: "v0"}

      opts = [
        group_id: "test-group",
        member_id: "member-123",
        generation_id: 5
      ]

      result = Heartbeat.Request.build_request(template, opts)

      assert result.group_id == "test-group"
      assert result.member_id == "member-123"
      assert result.generation_id == 5
      refute Map.has_key?(result, :group_instance_id)
    end

    test "routes V3+-like map to V3+ path (has group_instance_id)" do
      template = %{group_instance_id: nil}

      opts = [
        group_id: "test-group",
        member_id: "member-123",
        generation_id: 5,
        group_instance_id: "static-1"
      ]

      result = Heartbeat.Request.build_request(template, opts)

      assert result.group_id == "test-group"
      assert result.member_id == "member-123"
      assert result.generation_id == 5
      assert result.group_instance_id == "static-1"
    end

    test "V3+ path defaults group_instance_id to nil" do
      template = %{group_instance_id: nil}

      opts = [
        group_id: "test-group",
        member_id: "member-123",
        generation_id: 5
      ]

      result = Heartbeat.Request.build_request(template, opts)

      assert result.group_instance_id == nil
    end

    test "empty map routes to V0-V2 path" do
      template = %{}

      opts = [
        group_id: "test-group",
        member_id: "member-123",
        generation_id: 1
      ]

      result = Heartbeat.Request.build_request(template, opts)

      assert result.group_id == "test-group"
      refute Map.has_key?(result, :group_instance_id)
    end
  end
end
