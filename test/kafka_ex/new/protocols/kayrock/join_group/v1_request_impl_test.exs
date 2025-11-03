defmodule KafkaEx.New.Protocols.Kayrock.JoinGroup.V1RequestImplTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Protocols.Kayrock.JoinGroup
  alias Kayrock.JoinGroup.V1

  describe "build_request/2 for V1" do
    test "builds request with all required fields including rebalance_timeout" do
      request = %V1.Request{}

      opts = [
        group_id: "test-group",
        session_timeout: 30000,
        rebalance_timeout: 60000,
        member_id: "member-123",
        group_protocols: [%{protocol_name: "assign", protocol_metadata: <<>>}]
      ]

      result = JoinGroup.Request.build_request(request, opts)

      assert result == %V1.Request{
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
      request = %V1.Request{}

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
      request = %V1.Request{}

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
end
