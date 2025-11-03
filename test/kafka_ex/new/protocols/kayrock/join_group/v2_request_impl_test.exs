defmodule KafkaEx.New.Protocols.Kayrock.JoinGroup.V2RequestImplTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Protocols.Kayrock.JoinGroup
  alias Kayrock.JoinGroup.V2

  describe "build_request/2 for V2" do
    test "builds request with all required fields including rebalance_timeout" do
      request = %V2.Request{}

      opts = [
        group_id: "test-group",
        session_timeout: 30000,
        rebalance_timeout: 60000,
        member_id: "member-123",
        group_protocols: [%{protocol_name: "assign", protocol_metadata: <<>>}]
      ]

      result = JoinGroup.Request.build_request(request, opts)

      assert result == %V2.Request{
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

    test "V2 request structure matches V1 (difference is in response)" do
      request = %V2.Request{}

      opts = [
        group_id: "test-group",
        session_timeout: 30000,
        rebalance_timeout: 60000,
        member_id: "member-123",
        group_protocols: []
      ]

      result = JoinGroup.Request.build_request(request, opts)

      # V2 request has same fields as V1
      assert result.group_id == "test-group"
      assert result.session_timeout == 30000
      assert result.rebalance_timeout == 60000
    end
  end
end
