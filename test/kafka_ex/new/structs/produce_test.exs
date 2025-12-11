defmodule KafkaEx.New.Structs.ProduceTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Structs.Produce

  describe "build/1" do
    test "builds produce with required fields" do
      result = Produce.build(topic: "test-topic", partition: 0, base_offset: 100)
      assert %Produce{topic: "test-topic", partition: 0, base_offset: 100} = result
    end

    test "builds produce with all fields" do
      result =
        Produce.build(
          topic: "test-topic",
          partition: 1,
          base_offset: 500,
          log_append_time: 1_234_567_890,
          log_start_offset: 0,
          throttle_time_ms: 10
        )

      assert %Produce{
               topic: "test-topic",
               partition: 1,
               base_offset: 500,
               log_append_time: 1_234_567_890,
               log_start_offset: 0,
               throttle_time_ms: 10
             } = result
    end

    test "builds produce with zero throttle_time_ms" do
      result = Produce.build(topic: "t", partition: 0, base_offset: 0, throttle_time_ms: 0)
      assert result.throttle_time_ms == 0
    end

    test "builds produce with nil optional fields when not provided" do
      result = Produce.build(topic: "t", partition: 0, base_offset: 0)

      assert result.log_append_time == nil
      assert result.log_start_offset == nil
      assert result.throttle_time_ms == nil
    end

    test "builds produce with negative log_append_time (broker not using LogAppendTime)" do
      result = Produce.build(topic: "t", partition: 0, base_offset: 0, log_append_time: -1)

      assert result.log_append_time == -1
    end

    test "raises when topic is missing" do
      assert_raise KeyError, ~r/key :topic not found/, fn ->
        Produce.build(partition: 0, base_offset: 0)
      end
    end

    test "raises when partition is missing" do
      assert_raise KeyError, ~r/key :partition not found/, fn ->
        Produce.build(topic: "t", base_offset: 0)
      end
    end

    test "raises when base_offset is missing" do
      assert_raise KeyError, ~r/key :base_offset not found/, fn ->
        Produce.build(topic: "t", partition: 0)
      end
    end

    test "ignores extra options" do
      result = Produce.build(topic: "t", partition: 0, base_offset: 0, extra: "ignored")
      refute Map.has_key?(result, :extra)
    end
  end
end
