defmodule KafkaEx.New.Protocols.Kayrock.LeaveGroup.V1RequestImplTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Protocols.Kayrock.LeaveGroup
  alias Kayrock.LeaveGroup.V1

  describe "build_request/2 for V1" do
    test "builds request with all required fields" do
      request = %V1.Request{}

      opts = [
        group_id: "test-group",
        member_id: "member-456"
      ]

      result = LeaveGroup.Request.build_request(request, opts)

      assert result == %V1.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "test-group",
               member_id: "member-456"
             }
    end

    test "builds request identical to v0" do
      v0_request = %Kayrock.LeaveGroup.V0.Request{}
      v1_request = %V1.Request{}

      opts = [
        group_id: "same-group",
        member_id: "same-member"
      ]

      v0_result = LeaveGroup.Request.build_request(v0_request, opts)
      v1_result = LeaveGroup.Request.build_request(v1_request, opts)

      # Both should have same field values (struct names differ)
      assert v0_result.group_id == v1_result.group_id
      assert v0_result.member_id == v1_result.member_id
    end

    test "preserves correlation_id and client_id" do
      request = %V1.Request{correlation_id: 100, client_id: "v1-client"}

      opts = [
        group_id: "test-group",
        member_id: "test-member"
      ]

      result = LeaveGroup.Request.build_request(request, opts)

      assert result.correlation_id == 100
      assert result.client_id == "v1-client"
    end

    test "handles empty strings" do
      request = %V1.Request{}

      opts = [
        group_id: "",
        member_id: ""
      ]

      result = LeaveGroup.Request.build_request(request, opts)

      assert result.group_id == ""
      assert result.member_id == ""
    end

    test "handles unicode characters" do
      request = %V1.Request{}

      opts = [
        group_id: "group-日本",
        member_id: "member-한국"
      ]

      result = LeaveGroup.Request.build_request(request, opts)

      assert result.group_id == "group-日本"
      assert result.member_id == "member-한국"
    end
  end
end
