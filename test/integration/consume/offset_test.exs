defmodule KafkaEx.Integration.Consume.OffsetTest do
  @moduledoc """
  Integration tests for offset handling in fetch operations.

  Tests the library's offset APIs - earliest_offset, latest_offset, and fetch with specific offsets.
  Kafka-specific behavior tests (retention, sequential offset semantics) have been consolidated
  as we trust Kafka's guarantees.
  """
  use ExUnit.Case, async: true
  @moduletag :consume

  import KafkaEx.TestHelpers
  import KafkaEx.IntegrationHelpers

  alias KafkaEx.Client
  alias KafkaEx.API

  setup do
    {:ok, args} = KafkaEx.build_worker_options([])
    {:ok, pid} = Client.start_link(args, :no_name)

    {:ok, %{client: pid}}
  end

  describe "earliest_offset/3" do
    test "returns 0 for new topic", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      {:ok, earliest} = API.earliest_offset(client, topic_name, 0)

      assert earliest == 0
    end

    test "fetch from earliest returns first message", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      messages = Enum.map(1..10, fn i -> %{value: "earliest-#{i}"} end)
      {:ok, _} = API.produce(client, topic_name, 0, messages)

      {:ok, earliest} = API.earliest_offset(client, topic_name, 0)
      {:ok, fetch_result} = API.fetch(client, topic_name, 0, earliest)

      assert length(fetch_result.records) >= 1
      assert hd(fetch_result.records).value == "earliest-1"
    end
  end

  describe "latest_offset/3" do
    test "returns offset after last message", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      messages = Enum.map(1..5, fn i -> %{value: "latest-#{i}"} end)
      {:ok, result} = API.produce(client, topic_name, 0, messages)

      {:ok, latest} = API.latest_offset(client, topic_name, 0)

      assert latest == result.base_offset + 5
    end

    test "equals earliest for empty topic", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      {:ok, earliest} = API.earliest_offset(client, topic_name, 0)
      {:ok, latest} = API.latest_offset(client, topic_name, 0)

      assert earliest == latest
      assert earliest == 0
    end

    test "advances after new messages", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      {:ok, _} = API.produce(client, topic_name, 0, [%{value: "initial"}])
      {:ok, latest_before} = API.latest_offset(client, topic_name, 0)

      {:ok, _} = API.produce(client, topic_name, 0, [%{value: "after-latest"}])
      {:ok, latest_after} = API.latest_offset(client, topic_name, 0)

      assert latest_after == latest_before + 1
    end
  end

  describe "fetch from specific offset" do
    test "returns message at exact offset", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      messages = Enum.map(1..10, fn i -> %{value: "specific-#{i}"} end)
      {:ok, result} = API.produce(client, topic_name, 0, messages)

      middle_offset = result.base_offset + 5
      {:ok, fetch_result} = API.fetch(client, topic_name, 0, middle_offset)

      assert hd(fetch_result.records).value == "specific-6"
      assert hd(fetch_result.records).offset == middle_offset
    end

    test "offset 0 works for new topic", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      {:ok, _} = API.produce(client, topic_name, 0, [%{value: "at-zero"}])
      {:ok, fetch_result} = API.fetch(client, topic_name, 0, 0)

      assert length(fetch_result.records) >= 1
      assert hd(fetch_result.records).offset == 0
    end
  end

  describe "offset tracking" do
    test "fetched records have correct offsets and values", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      messages = Enum.map(1..30, fn i -> %{value: "track-#{i}"} end)
      {:ok, result} = API.produce(client, topic_name, 0, messages)

      {:ok, fetch_result} = API.fetch(client, topic_name, 0, result.base_offset, max_bytes: 100_000)

      assert length(fetch_result.records) == 30

      # Verify records have expected offset/value pairs
      Enum.each(Enum.with_index(fetch_result.records), fn {record, index} ->
        assert record.offset == result.base_offset + index
        assert record.value == "track-#{index + 1}"
      end)
    end
  end

  describe "multi-partition offsets" do
    test "each partition has independent offsets", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name, partitions: 4)

      {:ok, r0} = API.produce(client, topic_name, 0, Enum.map(1..10, fn i -> %{value: "p0-#{i}"} end))
      {:ok, r1} = API.produce(client, topic_name, 1, Enum.map(1..20, fn i -> %{value: "p1-#{i}"} end))
      {:ok, r2} = API.produce(client, topic_name, 2, Enum.map(1..5, fn i -> %{value: "p2-#{i}"} end))
      {:ok, r3} = API.produce(client, topic_name, 3, Enum.map(1..15, fn i -> %{value: "p3-#{i}"} end))

      # Verify fetching from each partition works independently
      {:ok, f0} = API.fetch(client, topic_name, 0, r0.base_offset)
      {:ok, f1} = API.fetch(client, topic_name, 1, r1.base_offset)
      {:ok, f2} = API.fetch(client, topic_name, 2, r2.base_offset)
      {:ok, f3} = API.fetch(client, topic_name, 3, r3.base_offset)

      assert hd(f0.records).value == "p0-1"
      assert hd(f1.records).value == "p1-1"
      assert hd(f2.records).value == "p2-1"
      assert hd(f3.records).value == "p3-1"
    end
  end
end
