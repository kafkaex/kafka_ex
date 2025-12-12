defmodule KafkaEx.New.Protocols.Kayrock.SyncGroup.RequestTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Protocols.Kayrock.SyncGroup
  alias KafkaEx.New.Protocols.Kayrock.SyncGroup.RequestHelpers

  describe "RequestHelpers.extract_common_fields/1" do
    test "extracts all required fields from opts" do
      opts = [
        group_id: "test-group",
        generation_id: 5,
        member_id: "member-123",
        other: "value"
      ]

      result = RequestHelpers.extract_common_fields(opts)

      assert %{
               group_id: "test-group",
               generation_id: 5,
               member_id: "member-123",
               group_assignment: []
             } = result
    end

    test "extracts fields with empty strings and zero generation" do
      opts = [
        group_id: "",
        generation_id: 0,
        member_id: ""
      ]

      result = RequestHelpers.extract_common_fields(opts)

      assert result.group_id == ""
      assert result.generation_id == 0
      assert result.member_id == ""
    end

    test "extracts fields with group_assignment" do
      assignments = [
        %{member_id: "member-1", member_assignment: <<1, 2, 3>>},
        %{member_id: "member-2", member_assignment: <<4, 5, 6>>}
      ]

      opts = [
        group_id: "test-group",
        generation_id: 1,
        member_id: "member-123",
        group_assignment: assignments
      ]

      result = RequestHelpers.extract_common_fields(opts)

      assert result.group_assignment == assignments
    end

    test "uses empty list for group_assignment when not provided" do
      opts = [
        group_id: "test-group",
        generation_id: 1,
        member_id: "member-123"
      ]

      result = RequestHelpers.extract_common_fields(opts)

      assert result.group_assignment == []
    end

    test "extracts fields with long strings" do
      long_group = String.duplicate("a", 1000)
      long_member = String.duplicate("b", 1000)

      opts = [
        group_id: long_group,
        generation_id: 999,
        member_id: long_member
      ]

      result = RequestHelpers.extract_common_fields(opts)

      assert result.group_id == long_group
      assert result.generation_id == 999
      assert result.member_id == long_member
    end

    test "raises when group_id is missing" do
      opts = [generation_id: 1, member_id: "member-123"]

      assert_raise KeyError, fn ->
        RequestHelpers.extract_common_fields(opts)
      end
    end

    test "raises when generation_id is missing" do
      opts = [group_id: "test-group", member_id: "member-123"]

      assert_raise KeyError, fn ->
        RequestHelpers.extract_common_fields(opts)
      end
    end

    test "raises when member_id is missing" do
      opts = [group_id: "test-group", generation_id: 1]

      assert_raise KeyError, fn ->
        RequestHelpers.extract_common_fields(opts)
      end
    end

    test "ignores extra fields in opts" do
      opts = [
        group_id: "test-group",
        generation_id: 1,
        member_id: "member-123",
        extra_field: "ignored",
        another: 42
      ]

      result = RequestHelpers.extract_common_fields(opts)

      assert Enum.sort(Map.keys(result)) == [
               :generation_id,
               :group_assignment,
               :group_id,
               :member_id
             ]
    end
  end

  describe "V0 Request implementation" do
    test "builds request with all required fields" do
      request = %Kayrock.SyncGroup.V0.Request{}

      opts = [
        group_id: "test-group",
        generation_id: 5,
        member_id: "member-123"
      ]

      result = SyncGroup.Request.build_request(request, opts)

      assert result == %Kayrock.SyncGroup.V0.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "test-group",
               generation_id: 5,
               member_id: "member-123",
               group_assignment: []
             }
    end

    test "builds request with empty strings and zero generation" do
      request = %Kayrock.SyncGroup.V0.Request{}

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
      request = %Kayrock.SyncGroup.V0.Request{}

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

      assert result == %Kayrock.SyncGroup.V0.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "consumer-group-1",
               generation_id: 3,
               member_id: "leader-member",
               group_assignment: assignments
             }
    end

    test "preserves existing correlation_id and client_id if present" do
      request = %Kayrock.SyncGroup.V0.Request{correlation_id: 42, client_id: "my-client"}

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
      request = %Kayrock.SyncGroup.V0.Request{}

      opts = [
        group_id: "test-group-rocket",
        generation_id: 10,
        member_id: "member-cafe"
      ]

      result = SyncGroup.Request.build_request(request, opts)

      assert result.group_id == "test-group-rocket"
      assert result.member_id == "member-cafe"
    end

    test "handles long string values" do
      request = %Kayrock.SyncGroup.V0.Request{}
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
      request = %Kayrock.SyncGroup.V0.Request{}

      opts = [
        group_id: "test-group",
        generation_id: 2_147_483_647,
        member_id: "member-123"
      ]

      result = SyncGroup.Request.build_request(request, opts)

      assert result.generation_id == 2_147_483_647
    end
  end

  describe "V1 Request implementation" do
    test "builds request with all required fields" do
      request = %Kayrock.SyncGroup.V1.Request{}

      opts = [
        group_id: "test-group",
        generation_id: 5,
        member_id: "member-123"
      ]

      result = SyncGroup.Request.build_request(request, opts)

      assert result == %Kayrock.SyncGroup.V1.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "test-group",
               generation_id: 5,
               member_id: "member-123",
               group_assignment: []
             }
    end

    test "builds request with empty strings and zero generation" do
      request = %Kayrock.SyncGroup.V1.Request{}

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
      request = %Kayrock.SyncGroup.V1.Request{}

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

      assert result == %Kayrock.SyncGroup.V1.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "consumer-group-1",
               generation_id: 3,
               member_id: "leader-member",
               group_assignment: assignments
             }
    end

    test "preserves existing correlation_id and client_id if present" do
      request = %Kayrock.SyncGroup.V1.Request{correlation_id: 42, client_id: "my-client"}

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
      request = %Kayrock.SyncGroup.V1.Request{}

      opts = [
        group_id: "test-group-rocket",
        generation_id: 10,
        member_id: "member-cafe"
      ]

      result = SyncGroup.Request.build_request(request, opts)

      assert result.group_id == "test-group-rocket"
      assert result.member_id == "member-cafe"
    end

    test "handles long string values" do
      request = %Kayrock.SyncGroup.V1.Request{}
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
      request = %Kayrock.SyncGroup.V1.Request{}

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
