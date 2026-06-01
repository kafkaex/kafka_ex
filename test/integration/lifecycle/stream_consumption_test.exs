defmodule KafkaEx.Integration.Lifecycle.StreamConsumptionTest do
  use ExUnit.Case, async: true
  @moduletag :lifecycle

  import KafkaEx.TestHelpers
  import KafkaEx.IntegrationHelpers

  alias KafkaEx.Client
  alias KafkaEx.API
  alias KafkaEx.Messages.RecordMetadata

  setup do
    {:ok, args} = KafkaEx.build_worker_options([])
    {:ok, pid} = Client.start_link(args, :no_name)

    {:ok, %{client: pid}}
  end

  describe "fetch_all/4" do
    test "fetches all messages from beginning", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      messages = Enum.map(1..20, fn i -> %{value: "fetch-all-#{i}"} end)
      {:ok, _} = API.produce(client, topic_name, 0, messages)

      {:ok, result} = API.fetch_all(client, topic_name, 0, max_bytes: 1_000_000)

      assert length(result.records) == 20
      assert hd(result.records).value == "fetch-all-1"
      assert List.last(result.records).value == "fetch-all-20"
    end
  end

  describe "produce_one/5" do
    test "produces single message", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      {:ok, result} = API.produce_one(client, topic_name, 0, "single-message")

      assert result.base_offset >= 0

      {:ok, fetch_result} = API.fetch(client, topic_name, 0, result.base_offset, max_bytes: 100_000)
      assert hd(fetch_result.records).value == "single-message"
    end

    test "produce_one with options", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      {:ok, result} = API.produce_one(client, topic_name, 0, "with-options", required_acks: 1)

      assert result.base_offset >= 0
    end
  end

  describe "continuous polling pattern" do
    test "poll with new messages arriving", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      {:ok, result} = API.produce(client, topic_name, 0, Enum.map(1..10, fn i -> %{value: "initial-#{i}"} end))

      {:ok, fetch1} = API.fetch(client, topic_name, 0, result.base_offset, max_bytes: 100_000)
      assert length(fetch1.records) == 10

      {:ok, _} = API.produce(client, topic_name, 0, Enum.map(11..20, fn i -> %{value: "new-#{i}"} end))

      next_offset = List.last(fetch1.records).offset + 1
      {:ok, fetch2} = API.fetch(client, topic_name, 0, next_offset, max_bytes: 100_000)

      assert length(fetch2.records) == 10
      assert hd(fetch2.records).value == "new-11"
    end

    test "resume from saved offset", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      messages = Enum.map(1..50, fn i -> %{value: "resume-#{i}"} end)
      {:ok, result} = API.produce(client, topic_name, 0, messages)

      {:ok, fetch1} = API.fetch(client, topic_name, 0, result.base_offset, max_bytes: 100_000)
      first_20 = Enum.take(fetch1.records, 20)
      saved_offset = List.last(first_20).offset + 1

      {:ok, fetch2} = API.fetch(client, topic_name, 0, saved_offset, max_bytes: 100_000)

      assert hd(fetch2.records).value == "resume-21"
      assert length(fetch2.records) == 30
    end
  end

  describe "KafkaEx.Consumer.Stream with no_wait_at_logend (#357)" do
    alias KafkaEx.Consumer.Stream, as: ConsumerStream

    @tag timeout: 30_000
    test "consumes N produced messages and halts at logend within max_wait_time + buffer", %{
      client: client
    } do
      topic = generate_random_string()
      _ = create_topic(client, topic)

      messages = for i <- 1..20, do: %{value: "msg-#{i}"}
      {:ok, %RecordMetadata{}} = API.produce(client, topic, 0, messages)

      stream = %ConsumerStream{
        client: client,
        topic: topic,
        partition: 0,
        offset: 0,
        no_wait_at_logend: true,
        fetch_options: [max_wait_time: 2_000],
        api_versions: %{}
      }

      {duration_us, records} = :timer.tc(fn -> Enum.to_list(stream) end)
      duration_ms = div(duration_us, 1_000)

      assert length(records) == 20
      assert Enum.map(records, & &1.value) == Enum.map(messages, & &1.value)

      # Pre-fix: this exceeds ~5_000ms due to sync_timeout-vs-max_wait_time
      # mismatch causing 3 retries of ~1s recv timeouts each.
      # Post-fix: ~2_000-3_000ms (broker max_wait_time + parse).
      assert duration_ms < 10_000, "expected logend halt within 10s, took #{duration_ms}ms"
    end
  end
end
