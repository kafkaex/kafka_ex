defmodule KafkaEx.IntegrationHelpers do
  @moduledoc false

  alias KafkaEx.API

  @doc """
  Creates basic topic
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

    API.create_topics(client, topics, 10_000)
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
