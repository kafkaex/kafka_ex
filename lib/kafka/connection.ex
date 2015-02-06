defmodule Kafka.Connection do
  def connect_brokers(uri) when is_tuple(uri) do
    connect_brokers([uri])
  end

  def connect_brokers([]) do
    raise "Error cannot connect"
  end

  def connect_brokers(uris) when is_list(uris) do
    [{host, port} | rest] = uris
    case connect(host, port) do
      {:error, _}        -> connect_brokers(rest)
      {:ok, socket}      -> socket
    end
  end

  def connect(host, port) do
    host |> format_host |> :gen_tcp.connect(port, [:binary, {:packet, 4}])
  end

  def close(socket), do: :gen_tcp.close(socket)

  def send(message, socket), do: :gen_tcp.send(socket, message)

  defp format_host(host) do
    case Regex.scan(~r/\d+/, host) do
      nil -> to_char_list(host)
      match_data -> match_data |> List.flatten |> Enum.map(&String.to_integer/1) |> List.to_tuple
    end
  end
end
