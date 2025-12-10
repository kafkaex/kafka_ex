defmodule KafkaEx.New.Protocols.Kayrock.Heartbeat.V1RequestImplTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Protocols.Kayrock.Heartbeat
  alias Kayrock.Heartbeat.V1

  describe "build_request/2 for V1" do
    test "builds request with all required fields" do
      request = %V1.Request{}

      opts = [
        group_id: "test-group",
        member_id: "member-456",
        generation_id: 10
      ]

      result = Heartbeat.Request.build_request(request, opts)

      assert result == %V1.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "test-group",
               member_id: "member-456",
               generation_id: 10
             }
    end

    test "builds request identical to v0" do
      v0_request = %Kayrock.Heartbeat.V0.Request{}
      v1_request = %V1.Request{}

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
      request = %V1.Request{correlation_id: 100, client_id: "v1-client"}

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
