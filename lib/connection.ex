defmodule Kafka.Connection do
  def connect([]) do
    {:error, "no brokers available"}
  end

  def connect(broker_list) do
    [first | rest] = broker_list
    case connect(Enum.at(first, 0), Enum.at(first, 1)) do
      {:ok, socket} -> {:ok, socket}
      {:error, _}   -> connect(rest)
    end
  end

  defp connect(host, port) when is_binary(host) do
    connect(to_char_list(host), port)
  end

  defp connect(host, port) do
    case :gen_tcp.connect(host, port, [:binary, {:packet, 4}]) do
      {:ok, socket}    -> {:ok, socket}
      error            -> error
    end
  end

  def close(socket) do
    :gen_tcp.close(socket)
  end

  def send(socket, message) do
    :gen_tcp.send(socket, message)
    receive do
      {:tcp, _, data} -> data
    end
  end
end
