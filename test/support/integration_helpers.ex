defmodule KafkaEx.IntegrationHelpers do
  @moduledoc false

  @doc """
  Creates basic topic
  """
  def create_topic(client, topic_name, opts \\ []) do
    KafkaEx.create_topics(
      [
        %{
          topic: topic_name,
          num_partitions: Keyword.get(opts, :partitions, 1),
          replication_factor: 1,
          replica_assignment: [],
          config_entries: %{}
        }
      ],
      timeout: 10_000,
      worker_name: client
    )
  end

  @doc """
  Produce messages to a given topic
  """
  def partition_produce(client, topic_name, message, partition) do
    KafkaEx.produce(topic_name, partition, message, worker_name: client)
  end

  @doc """
  Connects topic & consumer group
  """
  def join_to_group(client, topic, consumer_group) do
    request = %KafkaEx.Protocol.JoinGroup.Request{
      group_name: consumer_group,
      member_id: "",
      topics: [topic],
      session_timeout: 6000
    }

    response = KafkaEx.join_group(request, worker_name: client, timeout: 10000)
    {response.member_id, response.generation_id}
  end
end
