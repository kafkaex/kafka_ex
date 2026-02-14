defmodule KafkaEx.Protocol.Kayrock.JoinGroup.RequestHelpersTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Protocol.Kayrock.JoinGroup.RequestHelpers

  describe "extract_common_fields/1" do
    test "extracts all required fields" do
      opts = [
        group_id: "my-group",
        session_timeout: 30_000,
        member_id: "",
        group_protocols: [%{name: "range", metadata: <<>>}]
      ]

      result = RequestHelpers.extract_common_fields(opts)

      assert result.group_id == "my-group"
      assert result.session_timeout == 30_000
      assert result.member_id == ""
      assert result.protocol_type == "consumer"
      assert length(result.group_protocols) == 1
    end

    test "uses default protocol_type when not provided" do
      opts = [
        group_id: "my-group",
        session_timeout: 30_000,
        member_id: "",
        group_protocols: []
      ]

      result = RequestHelpers.extract_common_fields(opts)

      assert result.protocol_type == "consumer"
    end

    test "allows custom protocol_type" do
      opts = [
        group_id: "my-group",
        session_timeout: 30_000,
        member_id: "",
        protocol_type: "custom",
        group_protocols: []
      ]

      result = RequestHelpers.extract_common_fields(opts)

      assert result.protocol_type == "custom"
    end

    test "raises on missing group_id" do
      assert_raise KeyError, fn ->
        RequestHelpers.extract_common_fields(session_timeout: 1000, member_id: "", group_protocols: [])
      end
    end

    test "raises on missing session_timeout" do
      assert_raise KeyError, fn ->
        RequestHelpers.extract_common_fields(group_id: "g", member_id: "", group_protocols: [])
      end
    end

    test "raises on missing member_id" do
      assert_raise KeyError, fn ->
        RequestHelpers.extract_common_fields(group_id: "g", session_timeout: 1000, group_protocols: [])
      end
    end

    test "raises on missing group_protocols and topics" do
      assert_raise KeyError, fn ->
        RequestHelpers.extract_common_fields(group_id: "g", session_timeout: 1000, member_id: "")
      end
    end

    test "builds group_protocols from topics when group_protocols not provided" do
      opts = [
        group_id: "my-group",
        session_timeout: 30_000,
        member_id: "",
        topics: ["topic-1", "topic-2"]
      ]

      result = RequestHelpers.extract_common_fields(opts)

      assert length(result.group_protocols) == 1
      [protocol] = result.group_protocols
      assert protocol.name == "assign"
      assert protocol.metadata.topics == ["topic-1", "topic-2"]
    end

    test "uses provided group_protocols over topics" do
      opts = [
        group_id: "my-group",
        session_timeout: 30_000,
        member_id: "",
        topics: ["ignored-topic"],
        group_protocols: [%{name: "custom", metadata: <<1, 2, 3>>}]
      ]

      result = RequestHelpers.extract_common_fields(opts)

      assert length(result.group_protocols) == 1
      [protocol] = result.group_protocols
      assert protocol.name == "custom"
      assert protocol.metadata == <<1, 2, 3>>
    end
  end

  describe "build_group_protocols/1" do
    test "builds single assign protocol with topics" do
      result = RequestHelpers.build_group_protocols(["topic-a", "topic-b"])

      assert length(result) == 1
      [protocol] = result
      assert protocol.name == "assign"
      assert %Kayrock.GroupProtocolMetadata{} = protocol.metadata
      assert protocol.metadata.topics == ["topic-a", "topic-b"]
    end

    test "handles empty topics list" do
      result = RequestHelpers.build_group_protocols([])

      assert length(result) == 1
      [protocol] = result
      assert protocol.metadata.topics == []
    end
  end

  describe "build_v0_request/2" do
    test "builds V0 request with all fields" do
      template = %{}

      opts = [
        group_id: "test-group",
        session_timeout: 30_000,
        member_id: "member-1",
        group_protocols: [%{name: "range", metadata: <<>>}]
      ]

      result = RequestHelpers.build_v0_request(template, opts)

      assert result.group_id == "test-group"
      assert result.session_timeout_ms == 30_000
      assert result.member_id == "member-1"
      assert result.protocol_type == "consumer"
      assert length(result.protocols) == 1
    end
  end

  describe "build_v1_or_v2_request/2" do
    test "builds V1/V2 request with rebalance_timeout" do
      template = %{}

      opts = [
        group_id: "test-group",
        session_timeout: 30_000,
        rebalance_timeout: 60_000,
        member_id: "member-1",
        group_protocols: [%{name: "range", metadata: <<>>}]
      ]

      result = RequestHelpers.build_v1_or_v2_request(template, opts)

      assert result.group_id == "test-group"
      assert result.session_timeout_ms == 30_000
      assert result.rebalance_timeout_ms == 60_000
      assert result.member_id == "member-1"
    end

    test "raises on missing rebalance_timeout" do
      template = %{}

      opts = [
        group_id: "test-group",
        session_timeout: 30_000,
        member_id: "member-1",
        group_protocols: []
      ]

      assert_raise KeyError, fn ->
        RequestHelpers.build_v1_or_v2_request(template, opts)
      end
    end
  end

  describe "build_v5_plus_request/2" do
    test "builds V5+ request with group_instance_id from opts" do
      template = %{}

      opts = [
        group_id: "test-group",
        session_timeout: 30_000,
        rebalance_timeout: 60_000,
        member_id: "member-1",
        group_instance_id: "static-instance-1",
        group_protocols: [%{name: "range", metadata: <<>>}]
      ]

      result = RequestHelpers.build_v5_plus_request(template, opts)

      assert result.group_id == "test-group"
      assert result.session_timeout_ms == 30_000
      assert result.rebalance_timeout_ms == 60_000
      assert result.member_id == "member-1"
      assert result.group_instance_id == "static-instance-1"
      assert length(result.protocols) == 1
    end

    test "defaults group_instance_id to nil when not provided" do
      template = %{}

      opts = [
        group_id: "test-group",
        session_timeout: 30_000,
        rebalance_timeout: 60_000,
        member_id: "member-1",
        group_protocols: []
      ]

      result = RequestHelpers.build_v5_plus_request(template, opts)

      assert result.group_instance_id == nil
    end

    test "includes all V1 fields (rebalance_timeout) plus group_instance_id" do
      template = %{}

      opts = [
        group_id: "test-group",
        session_timeout: 30_000,
        rebalance_timeout: 60_000,
        member_id: "member-1",
        group_instance_id: "my-instance",
        group_protocols: [%{name: "assign", metadata: <<0, 1, 2>>}]
      ]

      result = RequestHelpers.build_v5_plus_request(template, opts)

      # V0 fields
      assert result.group_id == "test-group"
      assert result.session_timeout_ms == 30_000
      assert result.member_id == "member-1"
      assert result.protocol_type == "consumer"
      assert result.protocols == [%{name: "assign", metadata: <<0, 1, 2>>}]
      # V1 field
      assert result.rebalance_timeout_ms == 60_000
      # V5 field
      assert result.group_instance_id == "my-instance"
    end

    test "raises on missing rebalance_timeout" do
      template = %{}

      opts = [
        group_id: "test-group",
        session_timeout: 30_000,
        member_id: "member-1",
        group_instance_id: "my-instance",
        group_protocols: []
      ]

      assert_raise KeyError, fn ->
        RequestHelpers.build_v5_plus_request(template, opts)
      end
    end
  end
end
