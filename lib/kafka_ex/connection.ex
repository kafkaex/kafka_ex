defmodule KafkaEx.Connection do
  def connect_brokers(uris, socket_map \\ %{})

  def connect_brokers([], socket_map) when socket_map == %{} do
    raise KafkaEx.ConnectionError, message: "Error: Cannot connect to any of the broker(s) provided"
  end

  def connect_brokers([], socket_map), do: socket_map

  def connect_brokers([{host, port} | rest], socket_map) do
    case connect(host, port) do
      {:error, _}        -> connect_brokers(rest, socket_map)
      {:ok, socket}      -> connect_brokers(rest, Map.put(socket_map, {host, port}, socket))
    end
  end

  def connect(host, port) do
    {host, port} = format_uri(host, port)
    :gen_tcp.connect(host, port, [:binary, {:packet, 4}])
  end

  def format_uri(host, port) when is_list(port) do
    format_uri(host, to_string(port))
  end

  def format_uri(host, port) when is_binary(port) do
    format_uri(host, String.to_integer(port))
  end

  def format_uri(host, port) when is_list(host) do
    format_uri(to_string(host), port)
  end

  def format_uri(host, port) when is_binary(host) and is_integer(port) do
    {format_host(host), port}
  end

  def format_uri(host, port), do: raise(KafkaEx.ConnectionError, message: "Error: Bad broker format '{#{host}, #{port}}'")

  def close(socket), do: :gen_tcp.close(socket)

  def send(message, socket), do: :gen_tcp.send(socket, message)

  def format_host(host) do
    case Regex.scan(~r/\d+/, host) do
      match_data = [_, _, _, _] -> match_data |> List.flatten |> Enum.map(&String.to_integer/1) |> List.to_tuple
      _ -> to_char_list(host)
    end
  end
end
