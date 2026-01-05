defmodule KafkaEx.Integration.Produce.ReliabilityTest do
  use ExUnit.Case, async: true
  @moduletag :produce

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

  describe "produce with different acks settings" do
    test "produce with acks=0 (fire and forget) succeeds", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      messages = [%{value: "acks-0-message"}]

      {:ok, result} = API.produce(client, topic_name, 0, messages, required_acks: 0)

      assert %RecordMetadata{} = result
      assert result.topic == topic_name
      assert result.partition == 0

      # Wait for message to arrive and verify via fetch
      Process.sleep(500)
      {:ok, latest} = API.latest_offset(client, topic_name, 0)
      assert latest >= 1
    end

    test "produce with acks=1 (leader only) succeeds", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      messages = [%{value: "acks-1-message"}]

      {:ok, result} = API.produce(client, topic_name, 0, messages, required_acks: 1)

      assert %RecordMetadata{} = result
      assert result.base_offset >= 0

      {:ok, fetch_result} = API.fetch(client, topic_name, 0, result.base_offset)
      assert hd(fetch_result.records).value == "acks-1-message"
    end

    test "produce with acks=-1 (all replicas) succeeds", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      messages = [%{value: "acks-all-message"}]

      {:ok, result} = API.produce(client, topic_name, 0, messages, required_acks: -1)

      assert %RecordMetadata{} = result
      assert result.base_offset >= 0

      {:ok, fetch_result} = API.fetch(client, topic_name, 0, result.base_offset)
      assert hd(fetch_result.records).value == "acks-all-message"
    end
  end

  describe "produce timeout handling" do
    test "produce with reasonable timeout succeeds", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      messages = Enum.map(1..100, fn i -> %{value: "timeout-#{i}"} end)

      {:ok, result} = API.produce(client, topic_name, 0, messages, timeout: 30_000)

      assert %RecordMetadata{} = result
      assert result.base_offset >= 0

      {:ok, latest} = API.latest_offset(client, topic_name, 0)
      assert latest >= result.base_offset + 100
    end
  end

  describe "produce under load" do
    test "concurrent produces don't lose messages", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      # 10 concurrent producers, each sending 100 messages
      tasks =
        Enum.map(1..10, fn producer_id ->
          Task.async(fn ->
            Enum.map(1..100, fn msg_id ->
              {:ok, _result} =
                API.produce(client, topic_name, 0, [%{value: "p#{producer_id}-m#{msg_id}"}])
            end)
          end)
        end)

      Task.await_many(tasks, 60_000)

      {:ok, latest} = API.latest_offset(client, topic_name, 0)
      {:ok, earliest} = API.earliest_offset(client, topic_name, 0)
      total_messages = latest - earliest

      assert total_messages == 1000
    end

    test "burst produce of many small batches succeeds", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      results =
        Enum.map(1..500, fn i ->
          API.produce(client, topic_name, 0, [%{value: "burst-#{i}"}])
        end)

      success_count = Enum.count(results, fn {status, _} -> status == :ok end)
      assert success_count == 500

      {:ok, latest} = API.latest_offset(client, topic_name, 0)
      assert latest >= 500
    end
  end

  describe "produce error handling" do
    test "produce to invalid partition returns error", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name, partitions: 2)

      result = API.produce(client, topic_name, 99, [%{value: "invalid partition"}])

      case result do
        {:error, _reason} ->
          :ok

        {:ok, _} ->
          flunk("Expected error for invalid partition")
      end
    end
  end
end
