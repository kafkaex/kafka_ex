defmodule KafkaEx.New.Adapter.HeartbeatTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Adapter
  alias KafkaEx.Protocol.Heartbeat.Request, as: HeartbeatRequest
  alias KafkaEx.Protocol.Heartbeat.Response, as: HeartbeatResponse
  alias Kayrock.Heartbeat

  describe "heartbeat_request/1 - legacy to new API" do
    test "converts legacy HeartbeatRequest to Kayrock V0 request" do
      legacy_request = %HeartbeatRequest{
        group_name: "test-group",
        member_id: "consumer-123",
        generation_id: 5
      }

      {kayrock_request, consumer_group} = Adapter.heartbeat_request(legacy_request)

      assert consumer_group == "test-group"

      assert kayrock_request == %Heartbeat.V0.Request{
               group_id: "test-group",
               member_id: "consumer-123",
               generation_id: 5
             }
    end

    test "handles empty member_id" do
      legacy_request = %HeartbeatRequest{
        group_name: "my-group",
        member_id: "",
        generation_id: 0
      }

      {kayrock_request, consumer_group} = Adapter.heartbeat_request(legacy_request)

      assert consumer_group == "my-group"
      assert kayrock_request.member_id == ""
      assert kayrock_request.generation_id == 0
    end

    test "handles generation_id 0" do
      legacy_request = %HeartbeatRequest{
        group_name: "group",
        member_id: "member",
        generation_id: 0
      }

      {kayrock_request, _consumer_group} = Adapter.heartbeat_request(legacy_request)

      assert kayrock_request.generation_id == 0
    end

    test "handles large generation_id" do
      legacy_request = %HeartbeatRequest{
        group_name: "group",
        member_id: "member",
        generation_id: 999_999
      }

      {kayrock_request, _consumer_group} = Adapter.heartbeat_request(legacy_request)

      assert kayrock_request.generation_id == 999_999
    end
  end

  describe "heartbeat_response/1 - new to legacy API (V0)" do
    test "converts successful V0 response to legacy format" do
      kayrock_response = %Heartbeat.V0.Response{
        error_code: 0
      }

      legacy_response = Adapter.heartbeat_response(kayrock_response)

      assert %HeartbeatResponse{error_code: :no_error} = legacy_response
    end

    test "converts V0 error response (unknown_member_id)" do
      kayrock_response = %Heartbeat.V0.Response{
        error_code: 25
      }

      legacy_response = Adapter.heartbeat_response(kayrock_response)

      assert %HeartbeatResponse{error_code: :unknown_member_id} = legacy_response
    end

    test "converts V0 error response (illegal_generation)" do
      kayrock_response = %Heartbeat.V0.Response{
        error_code: 22
      }

      legacy_response = Adapter.heartbeat_response(kayrock_response)

      assert %HeartbeatResponse{error_code: :illegal_generation} = legacy_response
    end

    test "converts V0 error response (rebalance_in_progress)" do
      kayrock_response = %Heartbeat.V0.Response{
        error_code: 27
      }

      legacy_response = Adapter.heartbeat_response(kayrock_response)

      assert %HeartbeatResponse{error_code: :rebalance_in_progress} = legacy_response
    end

    test "converts V0 error response (not_coordinator)" do
      kayrock_response = %Heartbeat.V0.Response{
        error_code: 16
      }

      legacy_response = Adapter.heartbeat_response(kayrock_response)

      assert %HeartbeatResponse{error_code: :not_coordinator} = legacy_response
    end

    test "converts V0 error response (coordinator_not_available)" do
      kayrock_response = %Heartbeat.V0.Response{
        error_code: 15
      }

      legacy_response = Adapter.heartbeat_response(kayrock_response)

      assert %HeartbeatResponse{error_code: :coordinator_not_available} = legacy_response
    end
  end

  describe "heartbeat_response/1 - new to legacy API (V1)" do
    test "converts successful V1 response to legacy format (ignores throttle_time_ms)" do
      kayrock_response = %Heartbeat.V1.Response{
        error_code: 0,
        throttle_time_ms: 100
      }

      legacy_response = Adapter.heartbeat_response(kayrock_response)

      assert %HeartbeatResponse{error_code: :no_error} = legacy_response
    end

    test "converts V1 response with zero throttle_time_ms" do
      kayrock_response = %Heartbeat.V1.Response{
        error_code: 0,
        throttle_time_ms: 0
      }

      legacy_response = Adapter.heartbeat_response(kayrock_response)

      assert %HeartbeatResponse{error_code: :no_error} = legacy_response
    end

    test "converts V1 error response (unknown_member_id) with throttle" do
      kayrock_response = %Heartbeat.V1.Response{
        error_code: 25,
        throttle_time_ms: 50
      }

      legacy_response = Adapter.heartbeat_response(kayrock_response)

      assert %HeartbeatResponse{error_code: :unknown_member_id} = legacy_response
    end

    test "converts V1 error response (illegal_generation) with throttle" do
      kayrock_response = %Heartbeat.V1.Response{
        error_code: 22,
        throttle_time_ms: 75
      }

      legacy_response = Adapter.heartbeat_response(kayrock_response)

      assert %HeartbeatResponse{error_code: :illegal_generation} = legacy_response
    end

    test "converts V1 error response (rebalance_in_progress) with throttle" do
      kayrock_response = %Heartbeat.V1.Response{
        error_code: 27,
        throttle_time_ms: 200
      }

      legacy_response = Adapter.heartbeat_response(kayrock_response)

      assert %HeartbeatResponse{error_code: :rebalance_in_progress} = legacy_response
    end

    test "converts V1 error response (not_coordinator) with throttle" do
      kayrock_response = %Heartbeat.V1.Response{
        error_code: 16,
        throttle_time_ms: 10
      }

      legacy_response = Adapter.heartbeat_response(kayrock_response)

      assert %HeartbeatResponse{error_code: :not_coordinator} = legacy_response
    end

    test "converts V1 error response (coordinator_not_available) with throttle" do
      kayrock_response = %Heartbeat.V1.Response{
        error_code: 15,
        throttle_time_ms: 25
      }

      legacy_response = Adapter.heartbeat_response(kayrock_response)

      assert %HeartbeatResponse{error_code: :coordinator_not_available} = legacy_response
    end
  end

  describe "round-trip conversion" do
    test "legacy request -> kayrock -> legacy response maintains structure" do
      # Start with legacy request
      legacy_request = %HeartbeatRequest{
        group_name: "test-group",
        member_id: "member-1",
        generation_id: 42
      }

      # Convert to Kayrock format
      {kayrock_request, consumer_group} = Adapter.heartbeat_request(legacy_request)

      # Verify conversion
      assert consumer_group == "test-group"
      assert kayrock_request.group_id == "test-group"
      assert kayrock_request.member_id == "member-1"
      assert kayrock_request.generation_id == 42

      # Simulate successful Kayrock response (V0)
      kayrock_response_v0 = %Heartbeat.V0.Response{error_code: 0}

      # Convert back to legacy
      legacy_response = Adapter.heartbeat_response(kayrock_response_v0)

      assert %HeartbeatResponse{error_code: :no_error} = legacy_response
    end

    test "legacy request -> kayrock V1 -> legacy response maintains error semantics" do
      # Start with legacy request
      legacy_request = %HeartbeatRequest{
        group_name: "test-group",
        member_id: "member-1",
        generation_id: 42
      }

      # Convert to Kayrock format (would be V0 request)
      {_kayrock_request, _consumer_group} = Adapter.heartbeat_request(legacy_request)

      # Simulate error Kayrock response (V1 with throttle)
      kayrock_response_v1 = %Heartbeat.V1.Response{
        error_code: 27,
        throttle_time_ms: 150
      }

      # Convert back to legacy
      legacy_response = Adapter.heartbeat_response(kayrock_response_v1)

      # Verify error code is preserved (throttle is ignored in legacy)
      assert %HeartbeatResponse{error_code: :rebalance_in_progress} = legacy_response
    end
  end
end
