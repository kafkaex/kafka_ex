defmodule KafkaEx.New.Protocols.Kayrock.LeaveGroup.RequestTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Protocols.Kayrock.LeaveGroup
  alias KafkaEx.New.Protocols.Kayrock.LeaveGroup.RequestHelpers

  describe "RequestHelpers.extract_common_fields/1" do
    test "extracts all required fields from opts" do
      opts = [
        group_id: "test-group",
        member_id: "member-123",
        other: "value"
      ]

      result = RequestHelpers.extract_common_fields(opts)

      assert %{
               group_id: "test-group",
               member_id: "member-123"
             } = result
    end

    test "extracts fields with empty strings" do
      opts = [
        group_id: "",
        member_id: ""
      ]

      result = RequestHelpers.extract_common_fields(opts)

      assert result.group_id == ""
      assert result.member_id == ""
    end

    test "extracts fields with long strings" do
      long_group = String.duplicate("a", 1000)
      long_member = String.duplicate("b", 1000)

      opts = [
        group_id: long_group,
        member_id: long_member
      ]

      result = RequestHelpers.extract_common_fields(opts)

      assert result.group_id == long_group
      assert result.member_id == long_member
    end

    test "raises when group_id is missing" do
      opts = [member_id: "member-123"]

      assert_raise KeyError, fn ->
        RequestHelpers.extract_common_fields(opts)
      end
    end

    test "raises when member_id is missing" do
      opts = [group_id: "test-group"]

      assert_raise KeyError, fn ->
        RequestHelpers.extract_common_fields(opts)
      end
    end

    test "ignores extra fields in opts" do
      opts = [
        group_id: "test-group",
        member_id: "member-123",
        extra_field: "ignored",
        another: 42
      ]

      result = RequestHelpers.extract_common_fields(opts)

      assert Enum.sort(Map.keys(result)) == [:group_id, :member_id]
    end
  end

  describe "V0 Request implementation" do
    test "builds request with all required fields" do
      request = %Kayrock.LeaveGroup.V0.Request{}

      opts = [
        group_id: "test-group",
        member_id: "member-123"
      ]

      result = LeaveGroup.Request.build_request(request, opts)

      assert result == %Kayrock.LeaveGroup.V0.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "test-group",
               member_id: "member-123"
             }
    end

    test "builds request with empty strings" do
      request = %Kayrock.LeaveGroup.V0.Request{}

      opts = [
        group_id: "",
        member_id: ""
      ]

      result = LeaveGroup.Request.build_request(request, opts)

      assert result.group_id == ""
      assert result.member_id == ""
    end

    test "builds request with different string values" do
      request = %Kayrock.LeaveGroup.V0.Request{}

      opts = [
        group_id: "consumer-group-1",
        member_id: "consumer-member-abc"
      ]

      result = LeaveGroup.Request.build_request(request, opts)

      assert result == %Kayrock.LeaveGroup.V0.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "consumer-group-1",
               member_id: "consumer-member-abc"
             }
    end

    test "preserves existing correlation_id and client_id if present" do
      request = %Kayrock.LeaveGroup.V0.Request{correlation_id: 42, client_id: "my-client"}

      opts = [
        group_id: "test-group",
        member_id: "test-member"
      ]

      result = LeaveGroup.Request.build_request(request, opts)

      assert result.correlation_id == 42
      assert result.client_id == "my-client"
    end

    test "handles long string values" do
      request = %Kayrock.LeaveGroup.V0.Request{}
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

  describe "V1 Request implementation" do
    test "builds request with all required fields" do
      request = %Kayrock.LeaveGroup.V1.Request{}

      opts = [
        group_id: "test-group",
        member_id: "member-456"
      ]

      result = LeaveGroup.Request.build_request(request, opts)

      assert result == %Kayrock.LeaveGroup.V1.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "test-group",
               member_id: "member-456"
             }
    end

    test "builds request identical to V0" do
      v0_request = %Kayrock.LeaveGroup.V0.Request{}
      v1_request = %Kayrock.LeaveGroup.V1.Request{}

      opts = [
        group_id: "same-group",
        member_id: "same-member"
      ]

      v0_result = LeaveGroup.Request.build_request(v0_request, opts)
      v1_result = LeaveGroup.Request.build_request(v1_request, opts)

      assert v0_result.group_id == v1_result.group_id
      assert v0_result.member_id == v1_result.member_id
    end

    test "preserves correlation_id and client_id" do
      request = %Kayrock.LeaveGroup.V1.Request{correlation_id: 100, client_id: "v1-client"}

      opts = [
        group_id: "test-group",
        member_id: "test-member"
      ]

      result = LeaveGroup.Request.build_request(request, opts)

      assert result.correlation_id == 100
      assert result.client_id == "v1-client"
    end

    test "handles empty strings" do
      request = %Kayrock.LeaveGroup.V1.Request{}

      opts = [
        group_id: "",
        member_id: ""
      ]

      result = LeaveGroup.Request.build_request(request, opts)

      assert result.group_id == ""
      assert result.member_id == ""
    end
  end
end
