defmodule KafkaEx.Protocol.Metadata do
  defmodule Request do
    defstruct topic: nil
    @type t :: %Request{topic: binary}
  end

  defmodule Response do
    defstruct brokers: [], topic_metadatas: []

    def broker_for_topic(metadata, brokers, topic, partition) do
      case Enum.find(metadata.topic_metadatas, &(topic == &1.topic)) do
        nil -> nil
        topic_metadata ->
          case Enum.find(topic_metadata.partition_metadatas, &(partition == &1.partition_id)) do
            nil -> nil
            lead_broker ->
              case Enum.find(metadata.brokers, &(lead_broker.leader == &1.node_id)) do
                nil -> nil
                broker -> case Enum.find(brokers, &(broker.host == &1.host && broker.port == &1.port)) do
                  nil -> nil
                  broker -> case Port.info(broker.socket) do
                    nil        -> nil
                    :undefined -> nil      # Note this return value was removed in Elixir 1.1
                    _          -> broker
                  end
                end
              end
          end
      end
    end
  end

  defmodule Broker do
    defstruct node_id: 0, host: "", port: 0, socket: nil
    @type t :: %Broker{node_id: non_neg_integer, host: binary, port: non_neg_integer, socket: nil | :gen_tcp.socket}

    def connected?(broker = %Broker{}) do
      broker.socket != nil
    end
  end

  defmodule TopicMetadata do
    defstruct error_code: 0, topic: nil, partition_metadatas: []
  end

  defmodule PartitionMetadata do
    defstruct error_code: 0, partition_id: nil, leader: -1, replicas: [], isrs: []
  end

  def create_request(correlation_id, client_id, ""), do: KafkaEx.Protocol.create_request(:metadata, correlation_id, client_id) <> << 0 :: 32-signed >>

  def create_request(correlation_id, client_id, topic) when is_binary(topic), do: create_request(correlation_id, client_id, [topic])

  def create_request(correlation_id, client_id, topics) when is_list(topics) do
    KafkaEx.Protocol.create_request(:metadata, correlation_id, client_id) <> << length(topics) :: 32-signed, topic_data(topics) :: binary >>
  end

  defp topic_data([]), do: ""

  defp topic_data([topic|topics]) do
    << byte_size(topic) :: 16-signed, topic :: binary >> <> topic_data(topics)
  end

  def parse_response(<< _correlation_id :: 32-signed, brokers_size :: 32-signed, rest :: binary >>) do
    {brokers, rest} = parse_brokers(brokers_size, rest, [])
    << topic_metadatas_size :: 32-signed, rest :: binary >> = rest
    %Response{brokers: brokers, topic_metadatas: parse_topic_metadatas(topic_metadatas_size, rest)}
  end

  defp parse_brokers(0, rest, brokers), do: {brokers, rest}

  defp parse_brokers(brokers_size, << node_id :: 32-signed, host_len :: 16-signed, host :: size(host_len)-binary, port :: 32-signed, rest :: binary >>, brokers) do
    parse_brokers(brokers_size-1, rest, [%Broker{node_id: node_id, host: host, port: port} | brokers])
  end

  defp parse_topic_metadatas(0, _), do: []

  defp parse_topic_metadatas(topic_metadatas_size, << error_code :: 16-signed, topic_len :: 16-signed, topic :: size(topic_len)-binary, partition_metadatas_size :: 32-signed, rest :: binary >>) do
    {partition_metadatas, rest} = parse_partition_metadatas(partition_metadatas_size, [], rest)
    [%TopicMetadata{error_code: error_code, topic: topic, partition_metadatas: partition_metadatas} | parse_topic_metadatas(topic_metadatas_size-1, rest)]
  end

  defp parse_partition_metadatas(0, partition_metadatas, rest), do: {partition_metadatas, rest}

  defp parse_partition_metadatas(partition_metadatas_size, partition_metadatas, << error_code :: 16-signed, partition_id :: 32-signed, leader :: 32-signed, rest :: binary >>) do
    {replicas, rest} =  parse_replicas(rest)
    {isrs, rest} =  parse_isrs(rest)
    parse_partition_metadatas(partition_metadatas_size-1, [%PartitionMetadata{error_code: error_code, partition_id: partition_id, leader: leader, replicas: replicas, isrs: isrs} | partition_metadatas], rest)
  end

  defp parse_replicas(<< num_replicas :: 32-signed, rest :: binary >>) do
    parse_int32_array(num_replicas, rest)
  end

  defp parse_isrs(<< num_isrs :: 32-signed, rest ::binary >>) do
    parse_int32_array([], num_isrs, rest)
  end

  defp parse_int32_array(array \\ [], num, data)

  defp parse_int32_array(array, 0, rest) do
    {Enum.reverse(array), rest}
  end

  defp parse_int32_array(array, num, << value :: 32-signed, rest :: binary >>) do
    parse_int32_array([value|array], num-1, rest)
  end
end
