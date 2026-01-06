defmodule KafkaEx.Network.NetworkClient do
  @moduledoc """
  KafkaEx implementation of Client used to connect to Kafka Broker
  """
  @behaviour KafkaEx.Network.Behaviour
  require Logger

  alias KafkaEx.Auth.Config, as: AuthConfig
  alias KafkaEx.Auth.SASL
  alias KafkaEx.Auth.SASL.VersionSupport
  alias KafkaEx.Network.Socket
  alias KafkaEx.Telemetry

  @impl true
  def create_socket(host, port, ssl_options \\ [], use_ssl \\ false, auth_opts \\ nil) do
    metadata = Telemetry.connection_metadata(host, port, use_ssl)

    Telemetry.span([:kafka_ex, :connection], metadata, fn ->
      do_create_socket(host, port, ssl_options, use_ssl, auth_opts)
    end)
  end

  defp do_create_socket(host, port, ssl_options, use_ssl, auth_opts) do
    case Socket.create(format_host(host), port, build_socket_options(ssl_options), use_ssl) do
      {:ok, socket} ->
        finish_socket_setup(socket, auth_opts, host, port)

      {:error, reason} ->
        Logger.error("Could not connect to #{inspect_broker(host, port)} because of error #{inspect(reason)}")
        {nil, %{success: false, error: reason}}
    end
  end

  defp finish_socket_setup(socket, auth_opts, host, port) do
    case maybe_authenticate_sasl(socket, auth_opts) do
      :ok ->
        :ok = Socket.setopts(socket, [:binary, {:packet, 4}, {:active, true}])
        {socket, %{success: true}}

      {:error, reason} ->
        _ = Socket.close(socket)
        Logger.error("SASL authentication failed for #{inspect_broker(host, port)}}: #{inspect(reason)}")
        {nil, %{success: false, error: reason}}
    end
  end

  @impl true
  def close_socket(nil), do: :ok
  def close_socket(socket), do: Socket.close(socket)

  @impl true
  def send_async_request(%{socket: nil}, _data) do
    {:error, :not_connected}
  end

  def send_async_request(broker, data) do
    socket = broker.socket

    case Socket.send(socket, data) do
      :ok ->
        :ok

      {_, reason} ->
        broker_str = inspect_broker(broker.host, broker.port)
        Logger.error("Asynchronously sending data to broker #{broker_str} failed with #{inspect(reason)}")
        reason
    end
  end

  @impl true
  def send_sync_request(%{socket: nil}, _data, _timeout) do
    {:error, :not_connected}
  end

  def send_sync_request(%{:socket => socket} = broker, data, timeout) do
    case Socket.setopts(socket, [:binary, {:packet, 4}, {:active, false}]) do
      :ok ->
        response =
          case Socket.send(socket, data) do
            :ok ->
              receive_response(socket, timeout, broker)

            {_, reason} ->
              broker_str = inspect_broker(broker.host, broker.port)
              Logger.error("Sending data to broker #{broker_str} failed with #{inspect(reason)}")
              Socket.close(socket)

              {:error, reason}
          end

        response

      {:error, reason} ->
        broker_str = inspect_broker(broker.host, broker.port)
        Logger.error("Setting socket options for broker #{broker_str} failed with #{inspect(reason)}")
        Socket.close(socket)
        {:error, reason}
    end
  end

  def send_sync_request(nil, _, _) do
    {:error, :no_broker}
  end

  defp receive_response(socket, timeout, broker) do
    case Socket.recv(socket, 0, timeout) do
      {:ok, data} ->
        :ok = Socket.setopts(socket, [:binary, {:packet, 4}, {:active, true}])
        data

      {:error, reason} ->
        Logger.error("Receiving data from #{inspect_broker(broker.host, broker.port)} failed with #{inspect(reason)}")
        Socket.close(socket)

        {:error, reason}
    end
  end

  @impl true
  def format_host(host) do
    case Regex.scan(~r/^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$/, host) do
      [match_data] = [[_, _, _, _, _]] ->
        match_data |> tl() |> List.flatten() |> Enum.map(&String.to_integer/1) |> List.to_tuple()

      _ ->
        String.to_charlist(host)
    end
  end

  defp maybe_authenticate_sasl(_socket, nil), do: :ok

  defp maybe_authenticate_sasl(socket, %AuthConfig{} = cfg) do
    case VersionSupport.validate_config(cfg, socket) do
      :ok -> SASL.authenticate(socket, cfg)
      {:error, reason} -> {:error, reason}
    end
  end

  defp build_socket_options([]), do: [:binary, {:packet, 4}, {:active, false}]
  defp build_socket_options(ssl_options), do: build_socket_options([]) ++ ssl_options

  defp inspect_broker(host, port), do: "broker #{inspect(host)}:#{inspect(port)}"
end
