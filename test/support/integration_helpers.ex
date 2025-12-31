defmodule KafkaEx.IntegrationHelpers do
  @moduledoc false

  alias KafkaEx.API

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
  Connects topic & consumer group
  """
  def join_to_group(client, topic, consumer_group) do
    group_protocols = [
      %{
        protocol_name: "assign",
        protocol_metadata: %Kayrock.GroupProtocolMetadata{
          topics: [topic]
        }
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
