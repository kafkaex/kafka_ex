defmodule KafkaEx.New.Protocols.Kayrock.JoinGroup.RequestTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Protocols.Kayrock.JoinGroup
  alias KafkaEx.New.Protocols.Kayrock.JoinGroup.RequestHelpers

  describe "RequestHelpers.extract_common_fields/1" do
    test "extracts all required fields" do
      opts = [
        group_id: "test-group",
        session_timeout: 30000,
        member_id: "member-123",
        protocol_type: "consumer",
        group_protocols: [%{protocol_name: "assign", protocol_metadata: <<>>}]
      ]

      result = RequestHelpers.extract_common_fields(opts)

      assert result.group_id == "test-group"
      assert result.session_timeout == 30000
      assert result.member_id == "member-123"
      assert result.protocol_type == "consumer"
      assert result.group_protocols == [%{protocol_name: "assign", protocol_metadata: <<>>}]
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
        group_protocols: [%{protocol_name: "assign", protocol_metadata: <<0, 1, 2>>}]
      ]

      result = RequestHelpers.build_v0_request(template, opts)

      assert result.group_id == "test-group"
      assert result.session_timeout == 30000
      assert result.member_id == "member-123"
      assert result.protocol_type == "consumer"
      assert result.group_protocols == [%{protocol_name: "assign", protocol_metadata: <<0, 1, 2>>}]
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
      assert result.session_timeout == 30000
      assert result.rebalance_timeout == 60000
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
        group_protocols: [%{protocol_name: "assign", protocol_metadata: <<>>}]
      ]

      result = JoinGroup.Request.build_request(request, opts)

      assert result == %Kayrock.JoinGroup.V0.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "test-group",
               session_timeout: 30000,
               member_id: "member-123",
               protocol_type: "consumer",
               group_protocols: [%{protocol_name: "assign", protocol_metadata: <<>>}]
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
        %{protocol_name: "range", protocol_metadata: <<1, 2, 3>>},
        %{protocol_name: "roundrobin", protocol_metadata: <<4, 5, 6>>}
      ]

      opts = [
        group_id: "test-group",
        session_timeout: 30000,
        member_id: "member-123",
        group_protocols: protocols
      ]

      result = JoinGroup.Request.build_request(request, opts)

      assert result.group_protocols == protocols
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

      assert result.session_timeout == 2_147_483_647
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
        group_protocols: [%{protocol_name: "assign", protocol_metadata: <<>>}]
      ]

      result = JoinGroup.Request.build_request(request, opts)

      assert result == %Kayrock.JoinGroup.V1.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "test-group",
               session_timeout: 30000,
               rebalance_timeout: 60000,
               member_id: "member-123",
               protocol_type: "consumer",
               group_protocols: [%{protocol_name: "assign", protocol_metadata: <<>>}]
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

      assert result.rebalance_timeout == 300_000
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
        group_protocols: [%{protocol_name: "assign", protocol_metadata: <<>>}]
      ]

      result = JoinGroup.Request.build_request(request, opts)

      assert result == %Kayrock.JoinGroup.V2.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "test-group",
               session_timeout: 30000,
               rebalance_timeout: 60000,
               member_id: "member-123",
               protocol_type: "consumer",
               group_protocols: [%{protocol_name: "assign", protocol_metadata: <<>>}]
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
      assert result.session_timeout == 30000
      assert result.rebalance_timeout == 60000
    end
  end
end
