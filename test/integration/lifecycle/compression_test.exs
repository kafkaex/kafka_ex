defmodule KafkaEx.Integration.Lifecycle.CompressionTest do
  use ExUnit.Case
  @moduletag :lifecycle

  import KafkaEx.TestHelpers

  alias KafkaEx.API
  alias KafkaEx.Client

  @ssl_port 9092

  describe "gzip compression" do
    setup do
      {:ok, client} = start_client()
      {:ok, %{client: client}}
    end

    test "produce and fetch with gzip compression", %{client: client} do
      topic = generate_random_string()
      messages = Enum.map(1..100, fn i -> %{value: "gzip-message-#{i}"} end)

      {:ok, result} = API.produce(client, topic, 0, messages, compression: :gzip)

      assert result.base_offset >= 0

      # Fetch with enough max_bytes
      {:ok, fetch_result} = API.fetch(client, topic, 0, result.base_offset, max_bytes: 1_000_000)

      assert length(fetch_result.records) >= 100
    end

    test "gzip with small messages", %{client: client} do
      topic = generate_random_string()
      # Small messages may not compress well
      messages = Enum.map(1..10, fn i -> %{value: "#{i}"} end)

      {:ok, result} = API.produce(client, topic, 0, messages, compression: :gzip)

      assert result.base_offset >= 0

      {:ok, fetch_result} = API.fetch(client, topic, 0, result.base_offset)
      assert length(fetch_result.records) >= 10
    end

    test "gzip with highly compressible data", %{client: client} do
      topic = generate_random_string()
      # Repetitive data compresses very well
      value = String.duplicate("AAAAAAAAAA", 1000)
      messages = [%{value: value}]

      {:ok, result} = API.produce(client, topic, 0, messages, compression: :gzip)

      assert result.base_offset >= 0

      {:ok, fetch_result} = API.fetch(client, topic, 0, result.base_offset, max_bytes: 100_000)
      assert length(fetch_result.records) >= 1
      assert hd(fetch_result.records).value == value
    end
  end

  describe "snappy compression" do
    setup do
      {:ok, client} = start_client()
      {:ok, %{client: client}}
    end

    test "produce and fetch with snappy compression", %{client: client} do
      topic = generate_random_string()
      messages = Enum.map(1..50, fn i -> %{value: "snappy-message-#{i}"} end)

      {:ok, result} = API.produce(client, topic, 0, messages, compression: :snappy)

      assert result.base_offset >= 0

      {:ok, fetch_result} = API.fetch(client, topic, 0, result.base_offset, max_bytes: 1_000_000)
      assert length(fetch_result.records) >= 50
    end
  end

  describe "no compression" do
    setup do
      {:ok, client} = start_client()
      {:ok, %{client: client}}
    end

    test "produce and fetch without compression", %{client: client} do
      topic = generate_random_string()
      messages = Enum.map(1..100, fn i -> %{value: "no-compress-#{i}"} end)

      {:ok, result} = API.produce(client, topic, 0, messages)

      assert result.base_offset >= 0

      {:ok, fetch_result} = API.fetch(client, topic, 0, result.base_offset, max_bytes: 1_000_000)
      assert length(fetch_result.records) >= 100
    end
  end

  describe "max_bytes edge cases" do
    setup do
      {:ok, client} = start_client()
      topic = generate_random_string()

      # Produce messages with varying sizes
      messages =
        Enum.map(1..20, fn i ->
          # Create messages of varying sizes
          size = i * 100
          %{value: String.duplicate("x", size)}
        end)

      {:ok, produce_result} = API.produce(client, topic, 0, messages)

      {:ok, %{client: client, topic: topic, base_offset: produce_result.base_offset}}
    end

    test "fetch with very small max_bytes returns partial batch", %{
      client: client,
      topic: topic,
      base_offset: base_offset
    } do
      # Very small max_bytes - Kafka will return at least one message
      {:ok, result} = API.fetch(client, topic, 0, base_offset, max_bytes: 100)

      # Should get at least the first message (Kafka returns at least one)
      assert length(result.records) >= 1
    end

    test "fetch with exact message size max_bytes", %{
      client: client,
      topic: topic,
      base_offset: base_offset
    } do
      # First get a single message to know its size
      {:ok, first_result} = API.fetch(client, topic, 0, base_offset, max_bytes: 1000)
      first_msg = hd(first_result.records)
      msg_size = byte_size(first_msg.value || "")

      # Now fetch with that exact size - should get at least one
      {:ok, result} = API.fetch(client, topic, 0, base_offset, max_bytes: msg_size + 50)

      assert length(result.records) >= 1
    end

    test "fetch with large max_bytes returns all messages", %{client: client, topic: topic, base_offset: base_offset} do
      {:ok, result} = API.fetch(client, topic, 0, base_offset, max_bytes: 10_000_000)

      # Should get all 20 messages
      assert length(result.records) >= 20
    end
  end

  describe "binary data handling" do
    setup do
      {:ok, client} = start_client()
      {:ok, %{client: client}}
    end

    test "produce and fetch binary data with null bytes", %{client: client} do
      topic = generate_random_string()
      # Binary data with embedded null bytes
      binary_value = <<0, 1, 2, 0, 3, 4, 0, 5, 6, 0>>

      {:ok, result} = API.produce(client, topic, 0, [%{value: binary_value}])

      assert result.base_offset >= 0

      {:ok, fetch_result} = API.fetch(client, topic, 0, result.base_offset)
      assert hd(fetch_result.records).value == binary_value
    end

    test "produce and fetch UTF-8 data", %{client: client} do
      topic = generate_random_string()
      utf8_value = "Hello 世界 🌍 مرحبا"

      {:ok, result} = API.produce(client, topic, 0, [%{value: utf8_value}])

      {:ok, fetch_result} = API.fetch(client, topic, 0, result.base_offset)
      assert hd(fetch_result.records).value == utf8_value
    end

    test "produce and fetch all byte values", %{client: client} do
      topic = generate_random_string()
      # All possible byte values
      all_bytes = :binary.list_to_bin(Enum.to_list(0..255))

      {:ok, result} = API.produce(client, topic, 0, [%{value: all_bytes}])

      {:ok, fetch_result} = API.fetch(client, topic, 0, result.base_offset)
      assert hd(fetch_result.records).value == all_bytes
    end
  end

  # Helper functions

  defp start_client do
    {:ok, opts} =
      KafkaEx.build_worker_options(
        uris: [{"localhost", @ssl_port}],
        use_ssl: true,
        ssl_options: [verify: :verify_none]
      )

    Client.start_link(opts, :no_name)
  end
end
