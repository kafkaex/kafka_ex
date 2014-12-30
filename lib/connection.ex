defmodule Kafka.Connection do
  def connect([], _) do
    {:error, "no brokers available"}
  end

  def connect(broker_list, client_id) do
    [first | rest] = broker_list
    case :gen_tcp.connect(Enum.at(first, 0), Enum.at(first, 1), [:binary, {:packet, 4}]) do
      {:error, _}      -> connect(rest, client_id)
      {:ok, socket}    ->
        case get_metadata(socket, client_id) do
          {:ok, brokers, topic_metadata} ->
            {:ok, %{socket: socket, brokers: brokers, metadata: topic_metadata}}
          error -> error
        end
    end
  end

  defp get_metadata(socket, client_id) do
    :gen_tcp.send(socket, Kafka.Metadata.create_request(client_id, 1))
    receive do
      {:tcp, _, << correlation_id :: 32, data :: binary >>} ->
        Kafka.Metadata.parse_response(data)
    end
  end
end
