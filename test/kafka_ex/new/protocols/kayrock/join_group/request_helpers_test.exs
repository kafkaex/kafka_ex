defmodule KafkaEx.New.Protocols.Kayrock.JoinGroup.RequestHelpersTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Protocols.Kayrock.JoinGroup.RequestHelpers

  describe "extract_common_fields/1" do
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

  describe "build_v0_request/2" do
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

  describe "build_v1_or_v2_request/2" do
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
end
