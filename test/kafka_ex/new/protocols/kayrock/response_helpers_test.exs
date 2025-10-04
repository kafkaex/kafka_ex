defmodule KafkaEx.New.Protocols.Kayrock.ResponseHelpersTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Protocols.Kayrock.ResponseHelpers
  alias KafkaEx.New.Structs.Error, as: ErrorStruct

  describe "build_response/1" do
    test "returns ok tuple with list of offsets" do
      offsets = [%{offset: 100, partition: 0}, %{offset: 200, partition: 1}]

      result = ResponseHelpers.build_response(offsets)

      assert {:ok, ^offsets} = result
    end

    test "returns ok tuple with empty list" do
      result = ResponseHelpers.build_response([])

      assert {:ok, []} = result
    end

    test "builds error from error tuple" do
      error_tuple = {:unknown_topic_or_partition, "test-topic", 0}

      result = ResponseHelpers.build_response(error_tuple)

      assert {:error, %ErrorStruct{} = error} = result
      assert error.error == :unknown_topic_or_partition
    end

    test "builds error with topic and partition metadata" do
      error_tuple = {:offset_out_of_range, "my-topic", 5}

      {:error, error} = ResponseHelpers.build_response(error_tuple)

      assert error.error == :offset_out_of_range
    end
  end

  describe "fail_fast_iterate_topics/2" do
    test "iterates over topics and accumulates results" do
      topics_data = [
        %{topic: "topic1", partition_responses: [%{partition: 0}, %{partition: 1}]},
        %{topic: "topic2", partition_responses: [%{partition: 0}]}
      ]

      parser_fn = fn topic, partitions ->
        results = Enum.map(partitions, fn p -> {topic, p.partition} end)
        {:ok, results}
      end

      result = ResponseHelpers.fail_fast_iterate_topics(topics_data, parser_fn)

      # Results are accumulated in reverse order (new items prepended)
      assert [{"topic2", 0}, {"topic1", 0}, {"topic1", 1}] = result
    end

    test "stops iteration on first error" do
      topics_data = [
        %{topic: "topic1", partition_responses: [%{partition: 0}]},
        %{topic: "bad-topic", partition_responses: [%{partition: 0}]},
        %{topic: "topic3", partition_responses: [%{partition: 0}]}
      ]

      parser_fn = fn topic, _partitions ->
        if topic == "bad-topic" do
          {:error, {:some_error, topic, 0}}
        else
          {:ok, {topic, :success}}
        end
      end

      result = ResponseHelpers.fail_fast_iterate_topics(topics_data, parser_fn)

      assert {:some_error, "bad-topic", 0} = result
    end

    test "handles empty topics list" do
      parser_fn = fn _topic, _partitions -> {:ok, :result} end

      result = ResponseHelpers.fail_fast_iterate_topics([], parser_fn)

      assert [] = result
    end

    test "accumulates single results as list" do
      topics_data = [
        %{topic: "topic1", partition_responses: [%{partition: 0}]}
      ]

      parser_fn = fn topic, _partitions -> {:ok, topic} end

      result = ResponseHelpers.fail_fast_iterate_topics(topics_data, parser_fn)

      assert ["topic1"] = result
    end

    test "flattens list results correctly" do
      topics_data = [
        %{topic: "topic1", partition_responses: [%{partition: 0}, %{partition: 1}]}
      ]

      parser_fn = fn _topic, partitions ->
        {:ok, Enum.map(partitions, & &1.partition)}
      end

      result = ResponseHelpers.fail_fast_iterate_topics(topics_data, parser_fn)

      assert [0, 1] = result
    end
  end

  describe "fail_fast_iterate_partitions/3" do
    test "iterates over partitions and returns ok tuple with results" do
      partitions_data = [
        %{partition: 0, offset: 100},
        %{partition: 1, offset: 200},
        %{partition: 2, offset: 300}
      ]

      parser_fn = fn topic, datum ->
        {:ok, {topic, datum.partition, datum.offset}}
      end

      result = ResponseHelpers.fail_fast_iterate_partitions(partitions_data, "test-topic", parser_fn)

      assert {:ok, [{"test-topic", 2, 300}, {"test-topic", 1, 200}, {"test-topic", 0, 100}]} = result
    end

    test "stops iteration on first error and returns error tuple" do
      partitions_data = [
        %{partition: 0, offset: 100},
        %{partition: 1, offset: -1},
        %{partition: 2, offset: 300}
      ]

      parser_fn = fn topic, datum ->
        if datum.offset == -1 do
          {:error, {:invalid_offset, topic, datum.partition}}
        else
          {:ok, {topic, datum.partition}}
        end
      end

      result = ResponseHelpers.fail_fast_iterate_partitions(partitions_data, "test-topic", parser_fn)

      assert {:error, {:invalid_offset, "test-topic", 1}} = result
    end

    test "handles empty partitions list" do
      parser_fn = fn _topic, _datum -> {:ok, :result} end

      result = ResponseHelpers.fail_fast_iterate_partitions([], "test-topic", parser_fn)

      assert {:ok, []} = result
    end

    test "accumulates single partition result" do
      partitions_data = [%{partition: 0, offset: 100}]

      parser_fn = fn topic, datum -> {:ok, {topic, datum.partition}} end

      result = ResponseHelpers.fail_fast_iterate_partitions(partitions_data, "my-topic", parser_fn)

      assert {:ok, [{"my-topic", 0}]} = result
    end

    test "handles list results from parser function" do
      partitions_data = [
        %{partition: 0, offsets: [100, 200]},
        %{partition: 1, offsets: [300]}
      ]

      parser_fn = fn _topic, datum ->
        {:ok, Enum.map(datum.offsets, &{datum.partition, &1})}
      end

      result = ResponseHelpers.fail_fast_iterate_partitions(partitions_data, "test-topic", parser_fn)

      # List results are concatenated, so order is [1's offsets, 0's offsets]
      assert {:ok, [{1, 300}, {0, 100}, {0, 200}]} = result
    end
  end

  describe "parse_time/1" do
    test "parses :latest as -1" do
      assert -1 = ResponseHelpers.parse_time(:latest)
    end

    test "parses :earliest as -2" do
      assert -2 = ResponseHelpers.parse_time(:earliest)
    end

    test "returns integer timestamp as-is" do
      assert 1_234_567_890 = ResponseHelpers.parse_time(1_234_567_890)
    end

    test "returns zero as-is" do
      assert 0 = ResponseHelpers.parse_time(0)
    end

    test "returns negative integer as-is" do
      assert -100 = ResponseHelpers.parse_time(-100)
    end

    test "converts DateTime to unix timestamp in milliseconds" do
      datetime = ~U[2023-01-15 10:30:45.123Z]
      expected = DateTime.to_unix(datetime, :millisecond)

      result = ResponseHelpers.parse_time(datetime)

      assert expected == result
      assert is_integer(result)
    end

    test "converts DateTime with different timezone" do
      {:ok, datetime} = DateTime.from_naive(~N[2023-06-20 14:22:33.456], "Etc/UTC")
      expected = DateTime.to_unix(datetime, :millisecond)

      result = ResponseHelpers.parse_time(datetime)

      assert expected == result
    end
  end
end
