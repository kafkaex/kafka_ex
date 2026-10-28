defmodule KafkaEx.Messages.TopicPartitions do
  @moduledoc """
  A set of partitions of one topic, identified by topic id (KIP-516).

  Used by the KIP-848 consumer group APIs (`KafkaEx.API.consumer_group_heartbeat/3`,
  `KafkaEx.API.describe_consumer_group/3`), which name topics by id.

    * `topic_id` - 16-byte topic id (see `KafkaEx.Cluster.Topic.topic_id/0`)
    * `topic_name` - the topic's name, or `nil` when it is not known.
      ConsumerGroupHeartbeat responses carry ids only; KafkaEx fills the name in
      from the client's cluster metadata when that topic is known there (see
      `KafkaEx.Cluster.ClusterMetadata.topic_name_by_id/2`). ConsumerGroupDescribe
      responses carry the name themselves.
    * `partitions` - partition numbers
  """

  @type t :: %__MODULE__{
          topic_id: <<_::128>>,
          topic_name: String.t() | nil,
          partitions: [non_neg_integer()]
        }

  defstruct topic_id: nil, topic_name: nil, partitions: []

  @doc """
  Builds a `TopicPartitions` from a map with `:topic_id`, `:partitions` and an
  optional `:topic_name`.
  """
  @spec build(map()) :: t()
  def build(%{topic_id: topic_id, partitions: partitions} = topic_partitions) do
    %__MODULE__{
      topic_id: topic_id,
      topic_name: blank_to_nil(Map.get(topic_partitions, :topic_name)),
      partitions: partitions || []
    }
  end

  defp blank_to_nil(""), do: nil
  defp blank_to_nil(name), do: name
end
