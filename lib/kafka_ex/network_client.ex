defmodule KafkaEx.NetworkClient do
  def create_socket(host, port) do
    case :gen_tcp.connect(format_host(host), port, [:binary, {:packet, 4}]) do
      {:ok, socket} -> socket
      _             -> nil
    end
  end

  def close_socket(socket), do: :gen_tcp.close(socket)

  def send_async_request(broker, data) do
    socket = broker.socket
    case :gen_tcp.send(socket, data) do
      :ok -> :ok
      error -> error
    end
  end

  def send_sync_request(broker, data, timeout \\ 1000) do
    socket = broker.socket
    case :gen_tcp.send(socket, data) do
      :ok ->
        receive do
          {:tcp, ^socket, data} -> data
          {:tcp_closed, ^socket} -> nil
        after
          timeout -> nil
        end
      _ -> nil
    end
  end

  def format_host(host) do
    case Regex.scan(~r/\d+/, host) do
      match_data = [_, _, _, _] -> match_data |> List.flatten |> Enum.map(&String.to_integer/1) |> List.to_tuple
      _ -> to_char_list(host)
    end
  end
end
