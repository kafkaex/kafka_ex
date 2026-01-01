defmodule KafkaEx.Messages.HeartbeatTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Messages.Heartbeat

  describe "build/1" do
    test "builds heartbeat with throttle_time_ms" do
      result = Heartbeat.build(throttle_time_ms: 100)

      assert %Heartbeat{throttle_time_ms: 100} = result
    end

    test "builds heartbeat with zero throttle_time_ms" do
      result = Heartbeat.build(throttle_time_ms: 0)

      assert result.throttle_time_ms == 0
    end

    test "builds heartbeat with nil when throttle_time_ms not provided" do
      result = Heartbeat.build()

      assert result.throttle_time_ms == nil
    end

    test "builds heartbeat ignoring extra options" do
      result = Heartbeat.build(throttle_time_ms: 50, extra: "ignored")

      assert %Heartbeat{throttle_time_ms: 50} = result
      refute Map.has_key?(result, :extra)
    end
  end
end
