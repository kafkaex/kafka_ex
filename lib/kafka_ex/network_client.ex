defmodule KafkaEx.NetworkClient do
  require Logger

  def create_socket(host, port) do
    case :gen_tcp.connect(format_host(host), port, [:binary, {:packet, 4}]) do
      {:ok, socket} ->
        Logger.log(:debug, "Succesfully connected to #{inspect(host)} on port #{inspect port}")
        socket
      _             ->
        Logger.log(:error, "Could not connect to broker #{inspect(host)} on port #{inspect port}")
        nil
    end
  end

  def close_socket(nil), do: :ok
  def close_socket(socket), do: :gen_tcp.close(socket)

  def send_async_request(broker, data) do
    socket = broker.socket
    case :gen_tcp.send(socket, data) do
      :ok -> :ok
      {_, reason} ->
        Logger.log(:error, "Sending data to broker #{inspect broker.host} on port #{inspect broker.port} failed with #{inspect reason}")
        reason
    end
  end

  def send_sync_request(broker, data, timeout \\ 1000) do
    socket = broker.socket
    case :gen_tcp.send(socket, data) do
      :ok ->
        receive do
          {:tcp, ^socket, data}  -> data
          {:tcp_closed, ^socket} ->
            Logger.log(:error, "Sending data to broker #{inspect broker.host}, #{inspect broker.port} failed because connection closed")
            nil
        after
          timeout ->
            Logger.log(:error, "Timed out sending data to broker #{inspect broker.host}, #{inspect broker.port}")
            nil
        end
      {_, reason} ->
        Logger.log(:error, "Sending data to broker #{inspect broker.host}, #{inspect broker.port} failed with #{inspect reason}")
        nil
    end
  end

  def format_host(host) do
    case Regex.scan(~r/\d+/, host) do
      match_data = [_, _, _, _] -> match_data |> List.flatten |> Enum.map(&String.to_integer/1) |> List.to_tuple
      _ -> to_char_list(host)
    end
  end
end
