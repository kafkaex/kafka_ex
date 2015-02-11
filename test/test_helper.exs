ExUnit.start()

ExUnit.configure exclude: [integration: true]

defmodule TestHelper do
  def mock_send(state, message) do
    handle(state, message)
  end

  # fetch
  defp handle({_correlation_id, _brokers, _topics}, << 1 :: 16, _rest :: binary >>) do
    send self(), {:tcp, nil, << >>}
    :ok
  end

  # metadata
  defp handle({correlation_id, brokers, topics}, << 3 :: 16, _rest :: binary >>) do
    send self(), {:tcp, nil, generate_metadata_response(correlation_id, brokers, topics)}
    :ok
  end

  defp handle(_state, _data) do
    # no-op
    :ok
  end

  def generate_metadata_response(correlation_id, brokers, topics_and_partitions) do
    << correlation_id :: 32 >> <>
    encode_brokers(brokers) <>
    encode_topics_and_partitions(topics_and_partitions)
  end

  defp encode_brokers(brokers) do
    << length(Map.keys(brokers)) :: 32 >> <>
    Enum.reduce(brokers,
                << >>,
                fn({id, {host, port}}, data) ->
                  data <>
                  << id :: 32,
                     String.length(host) :: 16,
                     host :: binary,
                     port :: 32 >>
                end)
  end

  defp encode_topics_and_partitions(topics_and_partitions) do
    << length(Map.keys(topics_and_partitions)) :: 32 >> <>
    Enum.reduce(topics_and_partitions,
                << >>,
                fn({topic_name, topic}, data) ->
                  data <>
                  << topic.error_code :: 16,
                     String.length(topic_name) :: 16,
                     topic_name :: binary >> <>
                  encode_partitions(topic.partitions)
                end)
  end

  defp encode_partitions(partitions) do
    << length(Map.keys(partitions)) :: 32 >> <>
    Enum.reduce(partitions,
                << >>,
                fn({id, partition_data}, data) ->
                  data <>
                  << partition_data.error_code :: 16,
                     id :: 32,
                     partition_data.leader :: 32 >> <>
                  encode_array(partition_data.replicas) <>
                  encode_array(partition_data.isrs)
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
