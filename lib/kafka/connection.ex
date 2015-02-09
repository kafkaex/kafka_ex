defmodule Kafka.Connection do
  def connect_brokers(uri) when is_tuple(uri) do
    connect_brokers([uri])
  end

  def connect_brokers([]) do
    raise Kafka.ConnectionError, message: "Error cannot connect"
  end

  def connect_brokers(uris) when is_list(uris) do
    [{host, port} | rest] = uris
    case connect(host, port) do
      {:error, _}        -> connect_brokers(rest)
      {:ok, socket}      -> socket
    end
  end

  def connect_brokers(_), do: raise(Kafka.ConnectionError, message: "Error bad broker details")

  def connect(host, port) when is_binary(host) and is_integer(port) do
    host |> format_host |> :gen_tcp.connect(port, [:binary, {:packet, 4}])
  end

  def connect(host, port), do: raise(Kafka.ConnectionError, message: "Error bad broker details")

  def close(socket), do: :gen_tcp.close(socket)

  def send(message, socket), do: :gen_tcp.send(socket, message)

  def format_host(host) do
    case Regex.scan(~r/\d+/, host) do
      match_data = [_, _, _, _] -> match_data |> List.flatten |> Enum.map(&String.to_integer/1) |> List.to_tuple
      _ -> to_char_list(host)
    end
  end
end
