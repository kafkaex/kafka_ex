defmodule KafkaEx.NetworkClient do
  require Logger
  alias KafkaEx.Protocol.Metadata.Broker

  @moduledoc false
  @spec create_socket(binary, non_neg_integer) :: nil | :gen_tcp.socket
  def create_socket(host, port) do
    case :gen_tcp.connect(format_host(host), port, [:binary, {:packet, 4}]) do
      {:ok, socket} ->
        Logger.log(:debug, "Succesfully connected to broker #{inspect(host)}:#{inspect port}")
        socket
      _             ->
        Logger.log(:error, "Could not connect to broker #{inspect(host)}:#{inspect port}")
        nil
    end
  end

  @spec close_socket(nil | :gen_tcp.socket) :: :ok
  def close_socket(nil), do: :ok
  def close_socket(socket), do: :gen_tcp.close(socket)

  @spec send_async_request(Broker.t, iodata) :: :ok | {:error, :closed | :inet.posix}
  def send_async_request(broker, data) do
    socket = broker.socket
    case :gen_tcp.send(socket, data) do
      :ok -> :ok
      {_, reason} ->
        Logger.log(:error, "Asynchronously sending data to broker #{inspect broker.host}:#{inspect broker.port} failed with #{inspect reason}")
        reason
    end
  end

  @spec send_sync_request(Broker.t, iodata, timeout) :: nil | iodata
  def send_sync_request(%{:socket => socket} = broker, data, timeout) do
    :ok = :inet.setopts(socket, [:binary, {:packet, 4}, {:active, false}])
    response = case :gen_tcp.send(socket, data) do
      :ok ->
        case :gen_tcp.recv(socket, 0, timeout) do
          {:ok, data} -> data
          {:error, reason} ->
            Logger.log(:error, "Receiving data from broker #{inspect broker.host}:#{inspect broker.port} failed with #{inspect reason}")
            nil
        end
      {_, reason} ->
        Logger.log(:error, "Sending data to broker #{inspect broker.host}:#{inspect broker.port} failed with #{inspect reason}")
        nil
    end

    :ok = :inet.setopts(socket, [:binary, {:packet, 4}, {:active, true}])
    response
  end

  @spec format_host(binary) :: char_list | :inet.ip_address
  def format_host(host) do
    case Regex.scan(~r/^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$/, host) do
      [match_data] = [[_, _, _, _, _]] -> match_data |> tl |> List.flatten |> Enum.map(&String.to_integer/1) |> List.to_tuple
      _ -> to_char_list(host)
    end
  end
end
