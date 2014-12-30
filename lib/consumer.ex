defmodule Kafka.Consumer do
  use GenServer

  def init({broker_list, client_id}) do
    case connect(broker_list) do
      {:error, err} -> IO.puts("Error: #{err}")
      {:ok, socket} ->
        case send_request(socket, create_metadata_request(client_id, 1)) do
          {:ok, brokers, topic_metadata} -> {:ok, %{socket: socket, brokers: brokers, metadata: topic_metadata}}
          error -> error
        end
    end
  end

  defp connect([]) do
    {:error, "no brokers available"}
  end

  defp connect(broker_list) do
    [first | rest] = broker_list
    case :gen_tcp.connect(Enum.at(first, 0), Enum.at(first, 1), [:binary, {:packet, 4}]) do
      {:error, _}      -> connect(rest)
      {:ok, socket}    -> {:ok, socket}
    end
  end

  defp send_request(socket, request) do
    :gen_tcp.send(socket, request)
    receive do
      {:tcp, _, << correlation_id :: 32, data :: binary >>} ->
        parse_metadata_response(data)
    end
  end

  defp create_metadata_request(client_id, correlation_id) do
    << 3 :: 16, 0 :: 16, correlation_id :: 32, String.length(client_id) :: 16 >> <>
      client_id <> << 0 :: 32 >>
  end

  defp parse_metadata_response(metadata) do
    << num_brokers :: 32, rest :: binary >> = metadata
    case parse_broker_list(num_brokers, rest) do
      {:ok, brokers, rest} ->
        << num_topic_metadata :: 32, rest :: binary >> = rest
        case parse_topic_metadata(num_topic_metadata, rest) do
          {:ok, topic_metadata} -> {:ok, brokers, topic_metadata}
          error                 -> error
        end
      error                ->
        IO.puts("Error - #{error}")
        error
    end
  end

  defp parse_broker_list(0, rest) do
    {:ok, [], rest}
  end

  defp parse_broker_list(num_brokers, data) do
    << node_id :: 32, host_len :: 16, host :: size(host_len)-binary, port :: 32, rest :: binary >> = data
    case parse_broker_list(num_brokers-1, rest) do
      {:ok, list, rest} -> {:ok, [%{node_id: node_id, host: host, port: port} | list], rest}
      error             -> error
    end
  end

  defp parse_topic_metadata(0, _) do
    {:ok, []}
  end

  defp parse_topic_metadata(num_topic_metadata, data) do
    << error_code :: 16, topic_len :: 16, topic :: size(topic_len)-binary, num_partitions :: 32, rest :: binary >> = data
    case parse_partition_metadata(num_partitions, rest) do
      {:ok, partition_metadata, rest} ->
        case parse_topic_metadata(num_topic_metadata-1, rest) do
          {:ok, topic_metadata} ->
            {:ok, [%{topic: topic, error_code: error_code, partition_metadata: partition_metadata} | topic_metadata]}
          error                 -> error
        end
      error                           -> error
    end
  end

  defp parse_partition_metadata(0, _) do
    {:ok, [], nil}
  end

  defp parse_partition_metadata(num_partitions, data) do
    << error_code :: 16, id :: 32, leader :: 32, num_replicas :: 32, rest :: binary >> = data
    case parse_int32_array(num_replicas, rest) do
      {:ok, replicas, rest} ->
        << num_isr :: 32, rest :: binary >> = rest
        case parse_int32_array(num_isr, rest) do
          {:ok, isrs, rest} ->
            case parse_partition_metadata(num_partitions-1, rest) do
              {:ok, partition_metadata, rest} ->
                {:ok, [%{partition_id: id, error_code: error_code, leader: leader, replicas: replicas, isrs: isrs} | partition_metadata], rest}
              error                     -> error
            end
          error             -> error
        end
      error                 -> error
    end
  end

  defp parse_int32_array(0, rest) do
    {:ok, [], rest}
  end

  defp parse_int32_array(num, data) do
    << value :: 32, rest :: binary >> = data
    case parse_int32_array(num-1, rest) do
      {:ok, values, rest} -> {:ok, [value | values], rest}
      error         -> error
    end
  end
end
