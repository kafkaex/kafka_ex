defmodule KafkaEx.New.Protocols.Kayrock.JoinGroup.V0RequestImplTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Protocols.Kayrock.JoinGroup
  alias Kayrock.JoinGroup.V0

  describe "build_request/2 for V0" do
    test "builds request with all required fields" do
      request = %V0.Request{}

      opts = [
        group_id: "test-group",
        session_timeout: 30000,
        member_id: "member-123",
        group_protocols: [%{protocol_name: "assign", protocol_metadata: <<>>}]
      ]

      result = JoinGroup.Request.build_request(request, opts)

      assert result == %V0.Request{
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
      request = %V0.Request{}

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
      request = %V0.Request{}

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
      request = %V0.Request{}

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
      request = %V0.Request{correlation_id: 42, client_id: "my-client"}

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

    test "handles unicode characters in group_id and member_id" do
      request = %V0.Request{}

      opts = [
        group_id: "test-group-🚀",
        session_timeout: 30000,
        member_id: "member-café",
        group_protocols: []
      ]

      result = JoinGroup.Request.build_request(request, opts)

      assert result.group_id == "test-group-🚀"
      assert result.member_id == "member-café"
    end

    test "handles long string values" do
      request = %V0.Request{}
      long_group = String.duplicate("a", 500)
      long_member = String.duplicate("b", 500)

      opts = [
        group_id: long_group,
        session_timeout: 30000,
        member_id: long_member,
        group_protocols: []
      ]

      result = JoinGroup.Request.build_request(request, opts)

      assert result.group_id == long_group
      assert result.member_id == long_member
    end

    test "handles large session_timeout" do
      request = %V0.Request{}

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
end
