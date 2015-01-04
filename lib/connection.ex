defmodule Kafka.Connection do
  def connect([]) do
    {:error, "no brokers available"}
  end

  def connect(broker_list) do
    [first | rest] = broker_list
    case connect(Enum.at(first, 0), Enum.at(first, 1)) do
      {:ok, connection} -> {:ok, connection}
      {:error, _}   -> connect(rest)
    end
  end

  def connect(host, port) when is_binary(host) do
    connect(to_char_list(host), port)
  end

  def connect(host, port) do
    case :gen_tcp.connect(host, port, [:binary, {:packet, 4}]) do
      {:ok, socket}    -> {:ok, %{correlation_id: 1, socket: socket}}
      error            -> error
    end
  end

  def close(connection) do
    :gen_tcp.close(connection.socket)
  end

  def send(connection, message) do
    :gen_tcp.send(connection.socket, message)
    receive do
      {:tcp, _, data} ->
        {%{connection | correlation_id: connection.correlation_id + 1}, data}
    end
  end
end
