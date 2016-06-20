defmodule KafkaEx.Network.Client do
  require Logger

  alias KafkaEx.Network.{TCP, Error}

  @spec connect(binary, non_neg_integer) :: nil | any
  def connect(host, port, proto \\ TCP) do
    case proto.connect(format_host(host), port) do
      {:ok, conn} ->
        Logger.log(:debug, "Succesfully connected to broker #{inspect host}:#{inspect port}")
        conn
      {:error, reason} ->
        error("Connection to broker", %{host: host, port: port}, reason)
    end
  end

  @spec close(nil | any) :: :ok
  def close(broker),                   do: close(broker, TCP)
  def close(%{socket: nil}, _proto),   do: :ok
  def close(%{socket: socket}, proto), do: proto.close(socket)

  @spec send_async_request(KafkaEx.Protocol.Metadata.Broker.t, iodata) :: :ok | {:error, :closed | :inet.posix}
  def send_async_request(broker, data, proto \\ TCP) do
    socket = broker.socket
    case proto.send(socket, data) do
      :ok ->
        :ok
      {_, reason} ->
        error("Asynchronously sending data to broker", broker, reason)
    end
  end

  @spec send_sync_request(KafkaEx.Protocol.Metadata.Broker.t, iodata, timeout) :: nil | iodata
  def send_sync_request(broker, data, timeout, proto \\ TCP) do
    case proto.send(broker.socket, data) do
      :ok ->
        case proto.recv(broker.socket, timeout) do
          {:ok, data} ->
            data
          {:error, reason} ->
            error("Receiving data from broker", broker, reason)
        end
      {_, reason} ->
        error("Sending data to broker", broker, reason)
    end
  end

  @spec format_host(binary) :: char_list | :inet.ip_address
  def format_host(host) do
    case Regex.scan(~r/^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$/, host) do
      [match_data] = [[_, _, _, _, _]] -> match_data |> tl |> List.flatten |> Enum.map(&String.to_integer/1) |> List.to_tuple
      _ -> to_char_list(host)
    end
  end

  defp error(action, %{host: host, port: port}, reason) do
    msg = "#{action} #{inspect host}:#{inspect port} failed with #{inspect reason}"
    Logger.log(:error, msg)
    raise Error, msg
  end

end

defmodule KafkaEx.Network.Error do
  defexception [:message]
  def defexception(message), do: %__MODULE__{message: message}
end
