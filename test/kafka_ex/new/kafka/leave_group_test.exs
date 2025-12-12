defmodule KafkaEx.New.Kafka.LeaveGroupTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Kafka.LeaveGroup

  describe "build/1" do
    test "builds leave_group with throttle_time_ms" do
      result = LeaveGroup.build(throttle_time_ms: 100)

      assert %LeaveGroup{throttle_time_ms: 100} = result
    end

    test "builds leave_group with zero throttle_time_ms" do
      result = LeaveGroup.build(throttle_time_ms: 0)

      assert result.throttle_time_ms == 0
    end

    test "builds leave_group with nil when throttle_time_ms not provided" do
      result = LeaveGroup.build()

      assert result.throttle_time_ms == nil
    end

    test "builds leave_group ignoring extra options" do
      result = LeaveGroup.build(throttle_time_ms: 50, extra: "ignored")

      assert %LeaveGroup{throttle_time_ms: 50} = result
      refute Map.has_key?(result, :extra)
    end
  end
end
