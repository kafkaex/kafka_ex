defmodule Kafka.Connection do
  def connect([], _) do
    {:error, "no brokers available"}
  end

  def connect(%{:host => host, :port => port}, client_id) do
    connect(host, port, client_id)
  end

  def connect(broker_list, client_id) do
    [[host, port] | rest] = broker_list
    case connect(host, port, client_id) do
      {:ok, connection} -> {:ok, Map.put(connection, :broker_list, broker_list)}
      {:error, _}       -> connect(rest, client_id)
    end
  end

  def connect(host, port, client_id) when is_binary(host) do
    connect(to_char_list(host), port, client_id)
  end

  def connect(host, port, client_id) do
    case :gen_tcp.connect(host, port, [:binary, {:packet, 4}]) do
      {:ok, socket}    -> {:ok, %{:correlation_id => 1, :client_id => client_id, :socket => socket}}
      error            -> error
    end
  end

  def close(connection) do
    :gen_tcp.close(connection.socket)
  end

  def send(connection, message) do
    :gen_tcp.send(connection.socket, message)
    |> receive(connection)
  end

  defp receive(:ok, connection) do
    receive do
      {:tcp, _, data} ->
        {:ok, %{connection | :correlation_id => connection.correlation_id + 1}, data}
    end
  end

  defp receive({:error, reason}, _connection) do
    {:error, reason}
  end
end
