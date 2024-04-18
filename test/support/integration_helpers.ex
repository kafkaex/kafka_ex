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
end
