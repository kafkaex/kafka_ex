defmodule KafkaEx.New.KafkaExAPI do
  @moduledoc false

  alias KafkaEx.ServerKayrock

  require Logger

  @type node_id :: non_neg_integer
  @type topic_name :: binary
  @type partition_id :: non_neg_integer
  @type consumer_group_name :: binary

  def latest_offset(client, topic, partition) do
    request = %Kayrock.ListOffsets.V1.Request{
      replica_id: -1,
      topics: [
        %{topic: topic, partitions: [%{partition: partition, timestamp: -1}]}
      ]
    }

    {:ok, resp} =
      ServerKayrock.kayrock_call(
        client,
        request,
        {:topic_partition, topic, partition}
      )

    [topic_resp] = resp.responses
    [%{error_code: error_code, offset: offset}] = topic_resp.partition_responses

    case error_code do
      0 -> {:ok, offset}
      _ -> {:error, Kayrock.ErrorCode.code_to_atom(error_code)}
    end
  end

  def topics_metadata(client, topics, allow_topic_creation \\ false) do
    GenServer.call(client, {:topic_metadata, topics, allow_topic_creation})
  end

  def cluster_metadata(client) do
    GenServer.call(client, :cluster_metadata)
  end

  def correlation_id(client) do
    GenServer.call(client, :correlation_id)
  end

  def set_consumer_group_for_auto_commit(client, consumer_group) do
    GenServer.call(
      client,
      {:set_consumer_group_for_auto_commit, consumer_group}
    )
  end
end
