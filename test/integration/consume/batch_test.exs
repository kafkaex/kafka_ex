defmodule KafkaEx.Integration.Consume.BatchTest do
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

  describe "fetch large batch" do
    test "fetch 1000 messages in single request", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      messages = Enum.map(1..1000, fn i -> %{value: "large-fetch-#{i}"} end)
      {:ok, result} = API.produce(client, topic_name, 0, messages)

      {:ok, fetch_result} = API.fetch(client, topic_name, 0, result.base_offset, max_bytes: 20_000_000)
      assert length(fetch_result.records) == 1000
    end

    @tag timeout: 120_000
    @tag :skip
    test "fetch 10000 messages (huge batch) - iterative", %{client: client} do
      # Skipped: This test requires large timeouts and may stress the cluster
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      # Produce in batches to avoid timeout
      total_messages = 10_000
      batch_size = 1000

      base_offset =
        Enum.reduce(0..(div(total_messages, batch_size) - 1), nil, fn batch_num, acc ->
          messages =
            Enum.map(1..batch_size, fn i ->
              msg_num = batch_num * batch_size + i
              %{value: "huge-batch-#{msg_num}"}
            end)

          {:ok, result} = API.produce(client, topic_name, 0, messages)
          if acc == nil, do: result.base_offset, else: acc
        end)

      # Fetch iteratively since max_bytes may limit single fetch
      all_records = fetch_all_iteratively(client, topic_name, 0, base_offset, 20, 10_000_000)

      assert length(all_records) == total_messages
    end
  end

  describe "fetch with size limits" do
    test "small max_bytes may return full batch (Kafka returns complete record batches)", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      messages = Enum.map(1..100, fn i -> %{value: "size-limit-#{i}"} end)
      {:ok, result} = API.produce(client, topic_name, 0, messages)
      {:ok, fetch_result} = API.fetch(client, topic_name, 0, result.base_offset, max_bytes: 500)

      assert length(fetch_result.records) >= 1
    end

    test "max_bytes smaller than single message still returns one message", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      large_message = String.duplicate("x", 1000)

      {:ok, result} = API.produce(client, topic_name, 0, [%{value: large_message}])
      {:ok, fetch_result} = API.fetch(client, topic_name, 0, result.base_offset, max_bytes: 100)

      assert length(fetch_result.records) >= 1
      assert hd(fetch_result.records).value == large_message
    end
  end

  describe "iterative fetch" do
    test "multiple fetches with offset tracking", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      messages = Enum.map(1..50, fn i -> %{value: "multi-fetch-#{i}"} end)
      {:ok, result} = API.produce(client, topic_name, 0, messages)

      # First fetch - get all messages
      {:ok, fetch1} = API.fetch(client, topic_name, 0, result.base_offset, max_bytes: 1_000_000)
      assert length(fetch1.records) == 50

      # Verify offsets are sequential
      last_record = List.last(fetch1.records)
      assert last_record.offset == result.base_offset + 49
      assert last_record.value == "multi-fetch-50"
    end

    test "fetch handles message boundaries correctly", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      # Produce messages of varying sizes
      messages =
        Enum.map(1..30, fn i ->
          padding = String.duplicate("x", rem(i * 17, 100))
          %{value: "boundary-#{i}-#{padding}"}
        end)

      {:ok, result} = API.produce(client, topic_name, 0, messages)
      {:ok, fetch_result} = API.fetch(client, topic_name, 0, result.base_offset, max_bytes: 100_000)

      assert length(fetch_result.records) == 30
      offsets = Enum.map(fetch_result.records, & &1.offset)
      assert length(offsets) == length(Enum.uniq(offsets))
    end
  end

  describe "fetch edge cases" do
    test "fetch with offset beyond latest returns empty records or error", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      {:ok, result} = API.produce(client, topic_name, 0, [%{value: "msg"}])
      fetch_result = API.fetch(client, topic_name, 0, result.base_offset + 1000, max_bytes: 100_000)

      # Kafka behavior varies - may return empty records or offset_out_of_range error
      case fetch_result do
        {:ok, fr} ->
          assert fr.records == [], "Expected empty records for offset beyond latest"

        {:error, :offset_out_of_range} ->
          :ok

        other ->
          flunk("Unexpected result: #{inspect(other)}")
      end
    end

    test "fetch preserves message metadata", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      # Produce message with key
      {:ok, result} = API.produce(client, topic_name, 0, [%{value: "value-with-meta", key: "meta-key"}])

      {:ok, fetch_result} = API.fetch(client, topic_name, 0, result.base_offset)

      record = hd(fetch_result.records)
      assert record.value == "value-with-meta"
      assert record.key == "meta-key"
      assert record.offset == result.base_offset
    end
  end

  # Helper function for iterative fetching
  defp fetch_all_iteratively(client, topic, partition, start_offset, max_iterations, max_bytes) do
    fetch_all_iteratively(client, topic, partition, start_offset, max_iterations, max_bytes, [])
  end

  defp fetch_all_iteratively(_client, _topic, _partition, _offset, 0, _max_bytes, acc) do
    Enum.reverse(acc)
  end

  defp fetch_all_iteratively(client, topic, partition, offset, remaining, max_bytes, acc) do
    {:ok, fetch_result} = API.fetch(client, topic, partition, offset, max_bytes: max_bytes)

    case fetch_result.records do
      [] ->
        Enum.reverse(acc)

      records ->
        last_offset = List.last(records).offset
        next_offset = last_offset + 1
        fetch_all_iteratively(client, topic, partition, next_offset, remaining - 1, max_bytes, records ++ acc)
    end
  end
end
