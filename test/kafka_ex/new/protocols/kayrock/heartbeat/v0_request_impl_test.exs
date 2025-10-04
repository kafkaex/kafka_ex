defmodule KafkaEx.New.Protocols.Kayrock.Heartbeat.V0RequestImplTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Protocols.Kayrock.Heartbeat
  alias Kayrock.Heartbeat.V0

  describe "build_request/2 for V0" do
    test "builds request with all required fields" do
      request = %V0.Request{}

      opts = [
        group_id: "test-group",
        member_id: "member-123",
        generation_id: 5
      ]

      result = Heartbeat.Request.build_request(request, opts)

      assert result == %V0.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "test-group",
               member_id: "member-123",
               generation_id: 5
             }
    end

    test "builds request with generation_id zero" do
      request = %V0.Request{}

      opts = [
        group_id: "new-group",
        member_id: "new-member",
        generation_id: 0
      ]

      result = Heartbeat.Request.build_request(request, opts)

      assert result.generation_id == 0
    end

    test "builds request with different string values" do
      request = %V0.Request{}

      opts = [
        group_id: "consumer-group-1",
        member_id: "consumer-member-abc",
        generation_id: 100
      ]

      result = Heartbeat.Request.build_request(request, opts)

      assert result == %V0.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "consumer-group-1",
               member_id: "consumer-member-abc",
               generation_id: 100
             }
    end

    test "preserves existing correlation_id and client_id if present" do
      request = %V0.Request{correlation_id: 42, client_id: "my-client"}

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
      request = %V0.Request{}

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
      request = %V0.Request{}

      opts = [
        group_id: "test-group",
        member_id: "test-member",
        generation_id: 2_147_483_647
      ]

      result = Heartbeat.Request.build_request(request, opts)

      assert result.generation_id == 2_147_483_647
    end
  end
end
