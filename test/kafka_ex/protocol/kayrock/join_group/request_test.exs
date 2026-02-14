defmodule KafkaEx.Protocol.Kayrock.JoinGroup.RequestTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Protocol.Kayrock.JoinGroup
  alias KafkaEx.Protocol.Kayrock.JoinGroup.RequestHelpers

  describe "RequestHelpers.extract_common_fields/1" do
    test "extracts all required fields" do
      opts = [
        group_id: "test-group",
        session_timeout: 30000,
        member_id: "member-123",
        protocol_type: "consumer",
        group_protocols: [%{name: "assign", metadata: <<>>}]
      ]

      result = RequestHelpers.extract_common_fields(opts)

      assert result.group_id == "test-group"
      assert result.session_timeout == 30000
      assert result.member_id == "member-123"
      assert result.protocol_type == "consumer"
      assert result.group_protocols == [%{name: "assign", metadata: <<>>}]
    end

    test "uses default protocol_type if not provided" do
      opts = [
        group_id: "test-group",
        session_timeout: 30000,
        member_id: "member-123",
        group_protocols: []
      ]

      result = RequestHelpers.extract_common_fields(opts)

      assert result.protocol_type == "consumer"
    end

    test "raises when group_id is missing" do
      opts = [
        session_timeout: 30000,
        member_id: "member-123",
        group_protocols: []
      ]

      assert_raise KeyError, fn ->
        RequestHelpers.extract_common_fields(opts)
      end
    end
  end

  describe "RequestHelpers.build_v0_request/2" do
    test "builds V0 request with all fields" do
      template = %{existing_field: "value"}

      opts = [
        group_id: "test-group",
        session_timeout: 30000,
        member_id: "member-123",
        protocol_type: "consumer",
        group_protocols: [%{name: "assign", metadata: <<0, 1, 2>>}]
      ]

      result = RequestHelpers.build_v0_request(template, opts)

      assert result.group_id == "test-group"
      assert result.session_timeout_ms == 30000
      assert result.member_id == "member-123"
      assert result.protocol_type == "consumer"
      assert result.protocols == [%{name: "assign", metadata: <<0, 1, 2>>}]
      assert result.existing_field == "value"
    end
  end

  describe "RequestHelpers.build_v1_or_v2_request/2" do
    test "builds V1/V2 request with rebalance_timeout" do
      template = %{existing_field: "value"}

      opts = [
        group_id: "test-group",
        session_timeout: 30000,
        rebalance_timeout: 60000,
        member_id: "member-123",
        protocol_type: "consumer",
        group_protocols: []
      ]

      result = RequestHelpers.build_v1_or_v2_request(template, opts)

      assert result.group_id == "test-group"
      assert result.session_timeout_ms == 30000
      assert result.rebalance_timeout_ms == 60000
      assert result.member_id == "member-123"
    end

    test "raises when rebalance_timeout is missing" do
      template = %{}

      opts = [
        group_id: "test-group",
        session_timeout: 30000,
        member_id: "member-123",
        group_protocols: []
      ]

      assert_raise KeyError, fn ->
        RequestHelpers.build_v1_or_v2_request(template, opts)
      end
    end
  end

  describe "V0 Request implementation" do
    test "builds request with all required fields" do
      request = %Kayrock.JoinGroup.V0.Request{}

      opts = [
        group_id: "test-group",
        session_timeout: 30000,
        member_id: "member-123",
        group_protocols: [%{name: "assign", metadata: <<>>}]
      ]

      result = JoinGroup.Request.build_request(request, opts)

      assert result == %Kayrock.JoinGroup.V0.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "test-group",
               session_timeout_ms: 30000,
               member_id: "member-123",
               protocol_type: "consumer",
               protocols: [%{name: "assign", metadata: <<>>}]
             }
    end

    test "builds request with empty member_id (new member)" do
      request = %Kayrock.JoinGroup.V0.Request{}

      opts = [
        group_id: "test-group",
        session_timeout: 30000,
        member_id: "",
        group_protocols: []
      ]

      result = JoinGroup.Request.build_request(request, opts)

      assert result.member_id == ""
    end

    test "builds request with custom protocol_type" do
      request = %Kayrock.JoinGroup.V0.Request{}

      opts = [
        group_id: "test-group",
        session_timeout: 30000,
        member_id: "member-123",
        protocol_type: "custom-protocol",
        group_protocols: []
      ]

      result = JoinGroup.Request.build_request(request, opts)

      assert result.protocol_type == "custom-protocol"
    end

    test "builds request with multiple group_protocols" do
      request = %Kayrock.JoinGroup.V0.Request{}

      protocols = [
        %{name: "range", metadata: <<1, 2, 3>>},
        %{name: "roundrobin", metadata: <<4, 5, 6>>}
      ]

      opts = [
        group_id: "test-group",
        session_timeout: 30000,
        member_id: "member-123",
        group_protocols: protocols
      ]

      result = JoinGroup.Request.build_request(request, opts)

      assert result.protocols == protocols
    end

    test "preserves existing correlation_id and client_id if present" do
      request = %Kayrock.JoinGroup.V0.Request{correlation_id: 42, client_id: "my-client"}

      opts = [
        group_id: "test-group",
        session_timeout: 30000,
        member_id: "member-123",
        group_protocols: []
      ]

      result = JoinGroup.Request.build_request(request, opts)

      assert result.correlation_id == 42
      assert result.client_id == "my-client"
    end

    test "handles large session_timeout" do
      request = %Kayrock.JoinGroup.V0.Request{}

      opts = [
        group_id: "test-group",
        session_timeout: 2_147_483_647,
        member_id: "member-123",
        group_protocols: []
      ]

      result = JoinGroup.Request.build_request(request, opts)

      assert result.session_timeout_ms == 2_147_483_647
    end
  end

  describe "V1 Request implementation" do
    test "builds request with all required fields including rebalance_timeout" do
      request = %Kayrock.JoinGroup.V1.Request{}

      opts = [
        group_id: "test-group",
        session_timeout: 30000,
        rebalance_timeout: 60000,
        member_id: "member-123",
        group_protocols: [%{name: "assign", metadata: <<>>}]
      ]

      result = JoinGroup.Request.build_request(request, opts)

      assert result == %Kayrock.JoinGroup.V1.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "test-group",
               session_timeout_ms: 30000,
               rebalance_timeout_ms: 60000,
               member_id: "member-123",
               protocol_type: "consumer",
               protocols: [%{name: "assign", metadata: <<>>}]
             }
    end

    test "handles different rebalance_timeout values" do
      request = %Kayrock.JoinGroup.V1.Request{}

      opts = [
        group_id: "test-group",
        session_timeout: 30000,
        rebalance_timeout: 300_000,
        member_id: "member-123",
        group_protocols: []
      ]

      result = JoinGroup.Request.build_request(request, opts)

      assert result.rebalance_timeout_ms == 300_000
    end

    test "raises error when rebalance_timeout is missing" do
      request = %Kayrock.JoinGroup.V1.Request{}

      opts = [
        group_id: "test-group",
        session_timeout: 30000,
        member_id: "member-123",
        group_protocols: []
      ]

      assert_raise KeyError, fn ->
        JoinGroup.Request.build_request(request, opts)
      end
    end
  end

  describe "V2 Request implementation" do
    test "builds request with all required fields including rebalance_timeout" do
      request = %Kayrock.JoinGroup.V2.Request{}

      opts = [
        group_id: "test-group",
        session_timeout: 30000,
        rebalance_timeout: 60000,
        member_id: "member-123",
        group_protocols: [%{name: "assign", metadata: <<>>}]
      ]

      result = JoinGroup.Request.build_request(request, opts)

      assert result == %Kayrock.JoinGroup.V2.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "test-group",
               session_timeout_ms: 30000,
               rebalance_timeout_ms: 60000,
               member_id: "member-123",
               protocol_type: "consumer",
               protocols: [%{name: "assign", metadata: <<>>}]
             }
    end

    test "V2 request structure matches V1" do
      request = %Kayrock.JoinGroup.V2.Request{}

      opts = [
        group_id: "test-group",
        session_timeout: 30000,
        rebalance_timeout: 60000,
        member_id: "member-123",
        group_protocols: []
      ]

      result = JoinGroup.Request.build_request(request, opts)

      assert result.group_id == "test-group"
      assert result.session_timeout_ms == 30000
      assert result.rebalance_timeout_ms == 60000
    end
  end

  describe "V3 Request implementation" do
    test "builds request with all required fields (same as V2)" do
      request = %Kayrock.JoinGroup.V3.Request{}

      opts = [
        group_id: "test-group",
        session_timeout: 30000,
        rebalance_timeout: 60000,
        member_id: "member-123",
        group_protocols: [%{name: "assign", metadata: <<>>}]
      ]

      result = JoinGroup.Request.build_request(request, opts)

      assert result == %Kayrock.JoinGroup.V3.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "test-group",
               session_timeout_ms: 30000,
               rebalance_timeout_ms: 60000,
               member_id: "member-123",
               protocol_type: "consumer",
               protocols: [%{name: "assign", metadata: <<>>}]
             }
    end

    test "preserves existing correlation_id and client_id" do
      request = %Kayrock.JoinGroup.V3.Request{correlation_id: 99, client_id: "client-v3"}

      opts = [
        group_id: "test-group",
        session_timeout: 30000,
        rebalance_timeout: 60000,
        member_id: "member-123",
        group_protocols: []
      ]

      result = JoinGroup.Request.build_request(request, opts)

      assert result.correlation_id == 99
      assert result.client_id == "client-v3"
    end
  end

  describe "V4 Request implementation" do
    test "builds request with all required fields (same as V3)" do
      request = %Kayrock.JoinGroup.V4.Request{}

      opts = [
        group_id: "test-group",
        session_timeout: 30000,
        rebalance_timeout: 60000,
        member_id: "member-123",
        group_protocols: [%{name: "assign", metadata: <<>>}]
      ]

      result = JoinGroup.Request.build_request(request, opts)

      assert result == %Kayrock.JoinGroup.V4.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "test-group",
               session_timeout_ms: 30000,
               rebalance_timeout_ms: 60000,
               member_id: "member-123",
               protocol_type: "consumer",
               protocols: [%{name: "assign", metadata: <<>>}]
             }
    end
  end

  describe "V5 Request implementation" do
    test "builds request with group_instance_id" do
      request = %Kayrock.JoinGroup.V5.Request{}

      opts = [
        group_id: "test-group",
        session_timeout: 30000,
        rebalance_timeout: 60000,
        member_id: "member-123",
        group_instance_id: "static-instance-1",
        group_protocols: [%{name: "assign", metadata: <<>>}]
      ]

      result = JoinGroup.Request.build_request(request, opts)

      assert result == %Kayrock.JoinGroup.V5.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "test-group",
               session_timeout_ms: 30000,
               rebalance_timeout_ms: 60000,
               member_id: "member-123",
               group_instance_id: "static-instance-1",
               protocol_type: "consumer",
               protocols: [%{name: "assign", metadata: <<>>}]
             }
    end

    test "defaults group_instance_id to nil for dynamic membership" do
      request = %Kayrock.JoinGroup.V5.Request{}

      opts = [
        group_id: "test-group",
        session_timeout: 30000,
        rebalance_timeout: 60000,
        member_id: "",
        group_protocols: []
      ]

      result = JoinGroup.Request.build_request(request, opts)

      assert result.group_instance_id == nil
    end

    test "preserves existing correlation_id and client_id" do
      request = %Kayrock.JoinGroup.V5.Request{correlation_id: 55, client_id: "client-v5"}

      opts = [
        group_id: "test-group",
        session_timeout: 30000,
        rebalance_timeout: 60000,
        member_id: "member-123",
        group_protocols: []
      ]

      result = JoinGroup.Request.build_request(request, opts)

      assert result.correlation_id == 55
      assert result.client_id == "client-v5"
    end
  end

  describe "V6 Request implementation (FLEX)" do
    test "builds request with group_instance_id (same fields as V5)" do
      request = %Kayrock.JoinGroup.V6.Request{}

      opts = [
        group_id: "test-group",
        session_timeout: 30000,
        rebalance_timeout: 60000,
        member_id: "member-123",
        group_instance_id: "static-instance-2",
        group_protocols: [%{name: "assign", metadata: <<>>}]
      ]

      result = JoinGroup.Request.build_request(request, opts)

      assert result.group_id == "test-group"
      assert result.session_timeout_ms == 30000
      assert result.rebalance_timeout_ms == 60000
      assert result.member_id == "member-123"
      assert result.group_instance_id == "static-instance-2"
      assert result.protocol_type == "consumer"
      assert result.protocols == [%{name: "assign", metadata: <<>>}]
    end

    test "defaults group_instance_id to nil" do
      request = %Kayrock.JoinGroup.V6.Request{}

      opts = [
        group_id: "test-group",
        session_timeout: 30000,
        rebalance_timeout: 60000,
        member_id: "",
        group_protocols: []
      ]

      result = JoinGroup.Request.build_request(request, opts)

      assert result.group_instance_id == nil
    end

    test "preserves tagged_fields default" do
      request = %Kayrock.JoinGroup.V6.Request{}

      opts = [
        group_id: "test-group",
        session_timeout: 30000,
        rebalance_timeout: 60000,
        member_id: "member-123",
        group_protocols: []
      ]

      result = JoinGroup.Request.build_request(request, opts)

      # tagged_fields is a V6 struct field that we don't set, should remain default
      assert result.tagged_fields == []
    end
  end

  describe "Cross-version consistency (V1-V6)" do
    @v1_plus_opts [
      group_id: "cross-version-group",
      session_timeout: 30000,
      rebalance_timeout: 60000,
      member_id: "member-cross",
      group_protocols: [%{name: "assign", metadata: <<1, 2, 3>>}]
    ]

    @v1_v4_versions [
      {%Kayrock.JoinGroup.V1.Request{}, "V1"},
      {%Kayrock.JoinGroup.V2.Request{}, "V2"},
      {%Kayrock.JoinGroup.V3.Request{}, "V3"},
      {%Kayrock.JoinGroup.V4.Request{}, "V4"}
    ]

    test "V1-V4 produce identical domain fields" do
      results =
        Enum.map(@v1_v4_versions, fn {template, _label} ->
          JoinGroup.Request.build_request(template, @v1_plus_opts)
        end)

      # All should have the same domain-relevant fields
      for result <- results do
        assert result.group_id == "cross-version-group"
        assert result.session_timeout_ms == 30000
        assert result.rebalance_timeout_ms == 60000
        assert result.member_id == "member-cross"
        assert result.protocol_type == "consumer"
        assert result.protocols == [%{name: "assign", metadata: <<1, 2, 3>>}]
      end
    end

    test "V5-V6 produce identical domain fields (with group_instance_id)" do
      v5_v6_opts = @v1_plus_opts ++ [group_instance_id: "static-id"]

      v5_result =
        JoinGroup.Request.build_request(%Kayrock.JoinGroup.V5.Request{}, v5_v6_opts)

      v6_result =
        JoinGroup.Request.build_request(%Kayrock.JoinGroup.V6.Request{}, v5_v6_opts)

      for result <- [v5_result, v6_result] do
        assert result.group_id == "cross-version-group"
        assert result.session_timeout_ms == 30000
        assert result.rebalance_timeout_ms == 60000
        assert result.member_id == "member-cross"
        assert result.group_instance_id == "static-id"
        assert result.protocol_type == "consumer"
        assert result.protocols == [%{name: "assign", metadata: <<1, 2, 3>>}]
      end
    end
  end

  describe "Any fallback Request implementation" do
    test "routes V0-like struct to V0 path" do
      # A map without rebalance_timeout_ms or group_instance_id
      template = %{some_field: "v0"}

      opts = [
        group_id: "test-group",
        session_timeout: 30000,
        member_id: "member-123",
        group_protocols: []
      ]

      result = JoinGroup.Request.build_request(template, opts)

      assert result.group_id == "test-group"
      assert result.session_timeout_ms == 30000
      assert result.member_id == "member-123"
      refute Map.has_key?(result, :rebalance_timeout_ms)
      refute Map.has_key?(result, :group_instance_id)
    end

    test "routes V1-V4-like struct to V1+ path" do
      # A map with rebalance_timeout_ms but no group_instance_id
      template = %{rebalance_timeout_ms: nil}

      opts = [
        group_id: "test-group",
        session_timeout: 30000,
        rebalance_timeout: 60000,
        member_id: "member-123",
        group_protocols: []
      ]

      result = JoinGroup.Request.build_request(template, opts)

      assert result.group_id == "test-group"
      assert result.session_timeout_ms == 30000
      assert result.rebalance_timeout_ms == 60000
      assert result.member_id == "member-123"
      refute Map.has_key?(result, :group_instance_id)
    end

    test "routes V5+-like struct to V5+ path" do
      # A map with both rebalance_timeout_ms and group_instance_id
      template = %{rebalance_timeout_ms: nil, group_instance_id: nil}

      opts = [
        group_id: "test-group",
        session_timeout: 30000,
        rebalance_timeout: 60000,
        member_id: "member-123",
        group_instance_id: "static-1",
        group_protocols: []
      ]

      result = JoinGroup.Request.build_request(template, opts)

      assert result.group_id == "test-group"
      assert result.session_timeout_ms == 30000
      assert result.rebalance_timeout_ms == 60000
      assert result.member_id == "member-123"
      assert result.group_instance_id == "static-1"
    end

    test "V5+ path defaults group_instance_id to nil" do
      template = %{rebalance_timeout_ms: nil, group_instance_id: nil}

      opts = [
        group_id: "test-group",
        session_timeout: 30000,
        rebalance_timeout: 60000,
        member_id: "member-123",
        group_protocols: []
      ]

      result = JoinGroup.Request.build_request(template, opts)

      assert result.group_instance_id == nil
    end
  end
end
