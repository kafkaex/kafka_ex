defmodule KafkaEx.New.Protocols.Kayrock.LeaveGroup.V0RequestImplTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Protocols.Kayrock.LeaveGroup
  alias Kayrock.LeaveGroup.V0

  describe "build_request/2 for V0" do
    test "builds request with all required fields" do
      request = %V0.Request{}

      opts = [
        group_id: "test-group",
        member_id: "member-123"
      ]

      result = LeaveGroup.Request.build_request(request, opts)

      assert result == %V0.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "test-group",
               member_id: "member-123"
             }
    end

    test "builds request with empty strings" do
      request = %V0.Request{}

      opts = [
        group_id: "",
        member_id: ""
      ]

      result = LeaveGroup.Request.build_request(request, opts)

      assert result.group_id == ""
      assert result.member_id == ""
    end

    test "builds request with different string values" do
      request = %V0.Request{}

      opts = [
        group_id: "consumer-group-1",
        member_id: "consumer-member-abc"
      ]

      result = LeaveGroup.Request.build_request(request, opts)

      assert result == %V0.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "consumer-group-1",
               member_id: "consumer-member-abc"
             }
    end

    test "preserves existing correlation_id and client_id if present" do
      request = %V0.Request{correlation_id: 42, client_id: "my-client"}

      opts = [
        group_id: "test-group",
        member_id: "test-member"
      ]

      result = LeaveGroup.Request.build_request(request, opts)

      assert result.correlation_id == 42
      assert result.client_id == "my-client"
    end

    test "handles unicode characters in group_id and member_id" do
      request = %V0.Request{}

      opts = [
        group_id: "test-group-🚀",
        member_id: "member-café"
      ]

      result = LeaveGroup.Request.build_request(request, opts)

      assert result.group_id == "test-group-🚀"
      assert result.member_id == "member-café"
    end

    test "handles long string values" do
      request = %V0.Request{}
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
end
