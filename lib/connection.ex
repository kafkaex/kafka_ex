defmodule Kafka.Connection do
  def connect([], _) do
    {:error, "no brokers available"}
  end

  def connect(broker_list) do
    [first | rest] = broker_list
    case :gen_tcp.connect(Enum.at(first, 0), Enum.at(first, 1), [:binary, {:packet, 4}]) do
      {:error, _}      -> connect(rest)
      {:ok, socket}    -> {:ok, socket}
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
