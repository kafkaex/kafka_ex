defmodule KafkaEx.Network.NetworkClient do
  @moduledoc """
  KafkaEx implementation of Client used to connect to Kafka Broker
  """

  alias KafkaEx.Network.Socket

  def connect(%{host: host, port: port}, socket_options, use_ssl, timeout) do
    Socket.create(format_host(host), port, socket_options, use_ssl, timeout)
  end

  def write(socket, data), do: Socket.send(socket, data)

  def read(socket, timeout), do: Socket.recv(socket, 0, timeout)

  def activate(socket), do: Socket.setopts(socket, [:binary, {:packet, 4}, {:active, :once}])

  def close(socket), do: Socket.close(socket)

  defp format_host(host) do
    case Regex.scan(~r/^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$/, host) do
      [match_data] = [[_, _, _, _, _]] ->
        match_data |> tl() |> List.flatten() |> Enum.map(&String.to_integer/1) |> List.to_tuple()

      _ ->
        String.to_charlist(host)
    end
  end
end
