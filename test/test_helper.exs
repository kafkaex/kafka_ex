ExUnit.start()

defmodule TestHelper do
  def generate_metadata_response(correlation_id, brokers, topics_and_partitions) do
    << correlation_id :: 32 >> <>
    encode_brokers(brokers) <>
    encode_topics_and_partitions(topics_and_partitions)
  end

  defp encode_brokers(brokers) do
    << length(brokers) :: 32 >> <>
    Enum.reduce(brokers,
                << >>,
                fn(broker, data) ->
                  data <>
                  << broker.node_id :: 32,
                     String.length(broker.host) :: 16,
                     broker.host :: binary,
                     broker.port :: 32 >>
                end)
  end

  defp encode_topics_and_partitions(topics_and_partitions) do
    << length(topics_and_partitions) :: 32 >> <>
    Enum.reduce(topics_and_partitions,
                << >>,
                fn(topic, data) ->
                  data <>
                  << topic.error_code :: 16,
                     String.length(topic.name) :: 16,
                     topic.name :: binary >> <>
                  encode_partitions(topic.partitions)
                end)
  end

  defp encode_partitions(partitions) do
    << length(partitions) :: 32 >> <>
    Enum.reduce(partitions,
                << >>,
                fn(partition, data) ->
                  data <>
                  << partition.error_code :: 16,
                     partition.id :: 32,
                     partition.leader :: 32 >> <>
                  encode_array(partition.replicas) <>
                  encode_array(partition.isrs)
                end)
  end

  defp encode_array(array) do
    << length(array) :: 32 >> <>
    Enum.reduce(array,
                << >>,
                fn(item, data) ->
                  data <>
                  << item :: 32 >>
                end)
  end
end
