defmodule KafkaEx.Integration.Consume.FetchVersionTest do
  use ExUnit.Case, async: true
  @moduletag :consume

  import KafkaEx.TestHelpers
  import KafkaEx.IntegrationHelpers

  alias KafkaEx.Client
  alias KafkaEx.API
  alias KafkaEx.Messages.Fetch

  setup do
    {:ok, args} = KafkaEx.build_worker_options([])
    {:ok, pid} = Client.start_link(args, :no_name)
    {:ok, %{client: pid}}
  end

  describe "Fetch V11 (highest supported version)" do
    test "basic produce-and-fetch round trip", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      messages = [
        %{key: "k1", value: "hello-v11"},
        %{key: "k2", value: "world-v11"},
        %{key: "k3", value: "fetch-v11"}
      ]

      {:ok, produce_result} = API.produce(client, topic_name, 0, messages)

      {:ok, fetch_result} =
        API.fetch(client, topic_name, 0, produce_result.base_offset, api_version: 11)

      assert %Fetch{} = fetch_result
      assert length(fetch_result.records) == 3

      values = Enum.map(fetch_result.records, & &1.value)
      assert values == ["hello-v11", "world-v11", "fetch-v11"]

      keys = Enum.map(fetch_result.records, & &1.key)
      assert keys == ["k1", "k2", "k3"]
    end

    test "returns valid domain struct fields", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      {:ok, produce_result} = API.produce(client, topic_name, 0, [%{value: "struct-fields-test"}])

      {:ok, fetch_result} =
        API.fetch(client, topic_name, 0, produce_result.base_offset, api_version: 11)

      assert %Fetch{} = fetch_result
      assert fetch_result.topic == topic_name
      assert fetch_result.partition == 0

      # high_watermark should be >= base_offset + number of messages produced
      assert is_integer(fetch_result.high_watermark)
      assert fetch_result.high_watermark >= produce_result.base_offset + 1

      # V4+ fields
      assert is_integer(fetch_result.last_stable_offset)
      assert fetch_result.last_stable_offset >= 0

      # V5+ field
      assert is_integer(fetch_result.log_start_offset)
      assert fetch_result.log_start_offset >= 0

      # V11+ field (KIP-392: preferred read replica)
      # On a single-node cluster this is typically -1 (no preference)
      assert is_integer(fetch_result.preferred_read_replica) or is_nil(fetch_result.preferred_read_replica)

      # throttle_time_ms should be present and non-negative
      assert is_integer(fetch_result.throttle_time_ms)
      assert fetch_result.throttle_time_ms >= 0
    end

    test "empty topic returns empty records", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      # min_bytes: 0 so broker responds immediately even when no data available
      # (default min_bytes: 1 + max_wait_time: 10s would exceed the 5s GenServer timeout)
      {:ok, fetch_result} = API.fetch(client, topic_name, 0, 0, api_version: 11, min_bytes: 0)

      assert %Fetch{} = fetch_result
      assert fetch_result.records == []
      assert is_integer(fetch_result.high_watermark)
      assert fetch_result.high_watermark >= 0
    end

    test "offset tracking with next_offset/1", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      # Produce 10 messages
      messages = Enum.map(1..10, fn i -> %{value: "offset-track-#{i}"} end)
      {:ok, produce_result} = API.produce(client, topic_name, 0, messages)
      base = produce_result.base_offset

      # Fetch first 5 using a smaller max_bytes to potentially limit results,
      # but Kafka may return full batch — just verify offset mechanics
      {:ok, fetch1} = API.fetch(client, topic_name, 0, base, api_version: 11, max_bytes: 1_000_000)

      assert length(fetch1.records) == 10
      assert Fetch.next_offset(fetch1) == base + 10

      # Fetch from next_offset should return empty (all consumed)
      # min_bytes: 0 so broker responds immediately when no data available
      {:ok, fetch2} = API.fetch(client, topic_name, 0, Fetch.next_offset(fetch1), api_version: 11, min_bytes: 0)

      assert fetch2.records == []

      # Fetch from middle offset
      {:ok, fetch3} = API.fetch(client, topic_name, 0, base + 5, api_version: 11, max_bytes: 1_000_000)

      assert length(fetch3.records) == 5
      assert hd(fetch3.records).value == "offset-track-6"
      assert List.last(fetch3.records).value == "offset-track-10"
      assert Fetch.next_offset(fetch3) == base + 10
    end
  end
end
