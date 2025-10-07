defmodule KafkaEx.New.Protocols.Kayrock.SyncGroup.V0RequestImplTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Protocols.Kayrock.SyncGroup
  alias Kayrock.SyncGroup.V0

  describe "build_request/2 for V0" do
    test "builds request with all required fields" do
      request = %V0.Request{}

      opts = [
        group_id: "test-group",
        generation_id: 5,
        member_id: "member-123"
      ]

      result = SyncGroup.Request.build_request(request, opts)

      assert result == %V0.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "test-group",
               generation_id: 5,
               member_id: "member-123",
               group_assignment: []
             }
    end

    test "builds request with empty strings and zero generation" do
      request = %V0.Request{}

      opts = [
        group_id: "",
        generation_id: 0,
        member_id: ""
      ]

      result = SyncGroup.Request.build_request(request, opts)

      assert result.group_id == ""
      assert result.generation_id == 0
      assert result.member_id == ""
    end

    test "builds request with group_assignment" do
      request = %V0.Request{}

      assignments = [
        %{member_id: "member-1", member_assignment: <<1, 2, 3>>},
        %{member_id: "member-2", member_assignment: <<4, 5, 6>>}
      ]

      opts = [
        group_id: "consumer-group-1",
        generation_id: 3,
        member_id: "leader-member",
        group_assignment: assignments
      ]

      result = SyncGroup.Request.build_request(request, opts)

      assert result == %V0.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "consumer-group-1",
               generation_id: 3,
               member_id: "leader-member",
               group_assignment: assignments
             }
    end

    test "preserves existing correlation_id and client_id if present" do
      request = %V0.Request{correlation_id: 42, client_id: "my-client"}

      opts = [
        group_id: "test-group",
        generation_id: 1,
        member_id: "test-member"
      ]

      result = SyncGroup.Request.build_request(request, opts)

      assert result.correlation_id == 42
      assert result.client_id == "my-client"
    end

    test "handles unicode characters in group_id and member_id" do
      request = %V0.Request{}

      opts = [
        group_id: "test-group-🚀",
        generation_id: 10,
        member_id: "member-café"
      ]

      result = SyncGroup.Request.build_request(request, opts)

      assert result.group_id == "test-group-🚀"
      assert result.member_id == "member-café"
    end

    test "handles long string values" do
      request = %V0.Request{}
      long_group = String.duplicate("a", 500)
      long_member = String.duplicate("b", 500)

      opts = [
        group_id: long_group,
        generation_id: 100,
        member_id: long_member
      ]

      result = SyncGroup.Request.build_request(request, opts)

      assert result.group_id == long_group
      assert result.member_id == long_member
    end

    test "handles large generation_id" do
      request = %V0.Request{}

      opts = [
        group_id: "test-group",
        generation_id: 2_147_483_647,
        member_id: "member-123"
      ]

      result = SyncGroup.Request.build_request(request, opts)

      assert result.generation_id == 2_147_483_647
    end
  end
end
