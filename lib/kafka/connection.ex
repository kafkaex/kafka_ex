defmodule Kafka.Connection do
  def connect_brokers(uri) when is_tuple(uri) do
    connect_brokers([uri])
  end

  def connect_brokers([]) do
    raise Kafka.ConnectionError, message: "Error: Cannot connect to any brokers provided"
  end

  def connect_brokers(uris) when is_list(uris) do
    [{host, port} | rest] = uris
    case connect(host, port) do
      {:error, _}        -> connect_brokers(rest)
      {:ok, socket}      -> {{host, port}, socket}
    end
  end

  def connect_brokers(uris), do: raise(Kafka.ConnectionError, message: "Error: Bad broker format '#{uris}'")

  def connect(host, port) when is_list(port) do
    connect(host, to_string(port))
  end

  def connect(host, port) when is_binary(port) do
    connect(host, String.to_integer(port))
  end

  def connect(host, port) when is_list(host) do
    connect(to_string(host), port)
  end

  def connect(host, port) when is_binary(host) and is_integer(port) do
    host |> format_host |> :gen_tcp.connect(port, [:binary, {:packet, 4}])
  end

  def connect(host, port), do: raise(Kafka.ConnectionError, message: "Error: Bad broker format '{#{host}, #{port}}'")

  def close(socket), do: :gen_tcp.close(socket)

  def send(message, socket), do: :gen_tcp.send(socket, message)

  def format_host(host) do
    case Regex.scan(~r/\d+/, host) do
      match_data = [_, _, _, _] -> match_data |> List.flatten |> Enum.map(&String.to_integer/1) |> List.to_tuple
      _ -> to_char_list(host)
    end
  end
end
