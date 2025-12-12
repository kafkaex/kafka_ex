defmodule KafkaEx.New.Protocols.Kayrock.Heartbeat.RequestTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Protocols.Kayrock.Heartbeat
  alias KafkaEx.New.Protocols.Kayrock.Heartbeat.RequestHelpers

  describe "RequestHelpers.extract_common_fields/1" do
    test "extracts all required fields from opts" do
      opts = [
        group_id: "test-group",
        member_id: "member-123",
        generation_id: 5,
        other: "value"
      ]

      result = RequestHelpers.extract_common_fields(opts)

      assert %{
               group_id: "test-group",
               member_id: "member-123",
               generation_id: 5
             } = result
    end

    test "extracts fields with generation_id zero" do
      opts = [
        group_id: "new-group",
        member_id: "new-member",
        generation_id: 0
      ]

      result = RequestHelpers.extract_common_fields(opts)

      assert result.generation_id == 0
    end

    test "extracts fields with large generation_id" do
      opts = [
        group_id: "test-group",
        member_id: "test-member",
        generation_id: 999_999
      ]

      result = RequestHelpers.extract_common_fields(opts)

      assert result.generation_id == 999_999
    end

    test "raises when group_id is missing" do
      opts = [member_id: "member-123", generation_id: 5]

      assert_raise KeyError, fn ->
        RequestHelpers.extract_common_fields(opts)
      end
    end

    test "raises when member_id is missing" do
      opts = [group_id: "test-group", generation_id: 5]

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

    test "ignores extra fields in opts" do
      opts = [
        group_id: "test-group",
        member_id: "member-123",
        generation_id: 5,
        extra_field: "ignored",
        another: 42
      ]

      result = RequestHelpers.extract_common_fields(opts)

      assert Enum.sort(Map.keys(result)) == [:generation_id, :group_id, :member_id]
    end
  end

  describe "V0 Request implementation" do
    test "builds request with all required fields" do
      request = %Kayrock.Heartbeat.V0.Request{}

      opts = [
        group_id: "test-group",
        member_id: "member-123",
        generation_id: 5
      ]

      result = Heartbeat.Request.build_request(request, opts)

      assert result == %Kayrock.Heartbeat.V0.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "test-group",
               member_id: "member-123",
               generation_id: 5
             }
    end

    test "builds request with generation_id zero" do
      request = %Kayrock.Heartbeat.V0.Request{}

      opts = [
        group_id: "new-group",
        member_id: "new-member",
        generation_id: 0
      ]

      result = Heartbeat.Request.build_request(request, opts)

      assert result.generation_id == 0
    end

    test "builds request with different string values" do
      request = %Kayrock.Heartbeat.V0.Request{}

      opts = [
        group_id: "consumer-group-1",
        member_id: "consumer-member-abc",
        generation_id: 100
      ]

      result = Heartbeat.Request.build_request(request, opts)

      assert result == %Kayrock.Heartbeat.V0.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "consumer-group-1",
               member_id: "consumer-member-abc",
               generation_id: 100
             }
    end

    test "preserves existing correlation_id and client_id if present" do
      request = %Kayrock.Heartbeat.V0.Request{correlation_id: 42, client_id: "my-client"}

      opts = [
        group_id: "test-group",
        member_id: "test-member",
        generation_id: 10
      ]

      result = Heartbeat.Request.build_request(request, opts)

      assert result.correlation_id == 42
      assert result.client_id == "my-client"
    end

    test "handles empty strings for group_id and member_id" do
      request = %Kayrock.Heartbeat.V0.Request{}

      opts = [
        group_id: "",
        member_id: "",
        generation_id: 1
      ]

      result = Heartbeat.Request.build_request(request, opts)

      assert result.group_id == ""
      assert result.member_id == ""
    end

    test "handles large generation_id values" do
      request = %Kayrock.Heartbeat.V0.Request{}

      opts = [
        group_id: "test-group",
        member_id: "test-member",
        generation_id: 2_147_483_647
      ]

      result = Heartbeat.Request.build_request(request, opts)

      assert result.generation_id == 2_147_483_647
    end
  end

  describe "V1 Request implementation" do
    test "builds request with all required fields" do
      request = %Kayrock.Heartbeat.V1.Request{}

      opts = [
        group_id: "test-group",
        member_id: "member-456",
        generation_id: 10
      ]

      result = Heartbeat.Request.build_request(request, opts)

      assert result == %Kayrock.Heartbeat.V1.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "test-group",
               member_id: "member-456",
               generation_id: 10
             }
    end

    test "builds request identical to V0" do
      v0_request = %Kayrock.Heartbeat.V0.Request{}
      v1_request = %Kayrock.Heartbeat.V1.Request{}

      opts = [
        group_id: "same-group",
        member_id: "same-member",
        generation_id: 7
      ]

      v0_result = Heartbeat.Request.build_request(v0_request, opts)
      v1_result = Heartbeat.Request.build_request(v1_request, opts)

      # Both should have same field values (struct names differ)
      assert v0_result.group_id == v1_result.group_id
      assert v0_result.member_id == v1_result.member_id
      assert v0_result.generation_id == v1_result.generation_id
    end

    test "preserves correlation_id and client_id" do
      request = %Kayrock.Heartbeat.V1.Request{correlation_id: 100, client_id: "v1-client"}

      opts = [
        group_id: "test-group",
        member_id: "test-member",
        generation_id: 5
      ]

      result = Heartbeat.Request.build_request(request, opts)

      assert result.correlation_id == 100
      assert result.client_id == "v1-client"
    end
  end
end
