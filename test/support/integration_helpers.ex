defmodule KafkaEx.IntegrationHelpers do
  @moduledoc false

  alias KafkaEx.API
  alias KafkaEx.Test.KayrockFixtures, as: Fixtures

  @doc """
  Creates basic topic and waits for it to be available in metadata.
  """
  def create_topic(client, topic_name, opts \\ []) do
    topics = [
      [
        topic: topic_name,
        num_partitions: Keyword.get(opts, :partitions, 1),
        replication_factor: 1,
        replica_assignment: [],
        config_entries: []
      ]
    ]

    result = API.create_topics(client, topics, 10_000)
    # Wait for topic to appear in metadata (propagation can take time)
    wait_for_topic_metadata(client, topic_name)
    result
  end

  defp wait_for_topic_metadata(client, topic_name, retries \\ 10) do
    case API.metadata(client, [topic_name], []) do
      {:ok, metadata} when map_size(metadata.topics) > 0 ->
        if Map.has_key?(metadata.topics, topic_name) do
          :ok
        else
          retry_or_fail(client, topic_name, retries)
        end

      _ ->
        retry_or_fail(client, topic_name, retries)
    end
  end

  defp retry_or_fail(_client, topic_name, 0) do
    raise "Timeout waiting for topic #{topic_name} to appear in metadata"
  end

  defp retry_or_fail(client, topic_name, retries) do
    :timer.sleep(100)
    wait_for_topic_metadata(client, topic_name, retries - 1)
  end

  @doc """
  Produce messages to a given topic
  """
  def partition_produce(client, topic_name, message, partition) do
    API.produce_one(client, topic_name, partition, message)
  end

  @doc """
  Waits for topic to be fully available in metadata with partitions ready.
  Returns the topic metadata map on success.
  """
  def wait_for_topic_in_metadata(client, topic_name, retries \\ 20) do
    {:ok, metadata} = API.metadata(client, [topic_name])
    topic = Map.get(metadata.topics, topic_name)

    if topic != nil do
      topic
    else
      if retries > 0 do
        Process.sleep(500)
        wait_for_topic_in_metadata(client, topic_name, retries - 1)
      else
        raise "Topic #{topic_name} not found in metadata after retries"
      end
    end
  end

  @doc """
  Waits for a consumer group supervisor to become active via polling.
  Replaces fragile Process.sleep calls in tests.
  """
  def wait_for_consumer_active(consumer_pid, timeout \\ 15_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_wait_for_active(consumer_pid, deadline)
  end

  defp do_wait_for_active(consumer_pid, deadline) do
    if System.monotonic_time(:millisecond) >= deadline do
      raise "Consumer group did not become active within timeout"
    end

    try do
      if KafkaEx.Consumer.ConsumerGroup.active?(consumer_pid) do
        # Extra settling time for fetch loop to start
        Process.sleep(500)
        :ok
      else
        Process.sleep(200)
        do_wait_for_active(consumer_pid, deadline)
      end
    catch
      :exit, _ ->
        Process.sleep(200)
        do_wait_for_active(consumer_pid, deadline)
    end
  end

  @doc """
  Connects topic & consumer group
  """
  def join_to_group(client, topic, consumer_group) do
    group_protocols = [
      %{
        name: "assign",
        metadata: Fixtures.group_protocol_metadata(topics: [topic])
      }
    ]

    opts = [
      topics: [topic],
      session_timeout: 6000,
      rebalance_timeout: 6000,
      group_protocols: group_protocols,
      timeout: 10_000
    ]

    {:ok, response} = API.join_group(client, consumer_group, "", opts)
    {response.member_id, response.generation_id}
  end
end
