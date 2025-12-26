defmodule KafkaEx.NetworkClient do
  @moduledoc """
  KafkaEx implementation of Client used to connect to Kafka Broker
  """
  @behaviour KafkaEx.NetworkClient.Behaviour

  require Logger

  alias KafkaEx.Auth.Config, as: AuthConfig
  alias KafkaEx.Auth.SASL
  alias KafkaEx.Auth.SASL.VersionSupport
  alias KafkaEx.Socket

  @impl true
  def create_socket(host, port, ssl_options \\ [], use_ssl \\ false, auth_opts \\ nil) do
    case Socket.create(format_host(host), port, build_socket_options(ssl_options), use_ssl) do
      {:ok, socket} ->
        case maybe_authenticate_sasl(socket, enrich_auth_opts(auth_opts, host, port)) do
          :ok ->
            :ok = Socket.setopts(socket, [:binary, {:packet, 4}, {:active, true}])
            socket

          {:error, reason} ->
            _ = Socket.close(socket)
            Logger.error("SASL authentication failed for #{inspect(host)}:#{inspect(port)}: #{inspect(reason)}")
            nil
        end

      {:error, reason} ->
        Logger.error(
          "Could not connect to broker #{inspect(host)}:#{inspect(port)} because of error #{inspect(reason)}"
        )

        nil
    end
  end

  @impl true
  def close_socket(nil), do: :ok
  def close_socket(socket), do: Socket.close(socket)

  @impl true
  def send_async_request(broker, data) do
    socket = broker.socket

    case Socket.send(socket, data) do
      :ok ->
        :ok

      {_, reason} ->
        broker_str = broker_address_str(broker)
        Logger.error("Asynchronously sending data to broker #{broker_str} failed with #{inspect(reason)}")

        reason
    end
  end

  @impl true
  def send_sync_request(%{:socket => socket} = broker, data, timeout) do
    case Socket.setopts(socket, [:binary, {:packet, 4}, {:active, false}]) do
      :ok ->
        response =
          case Socket.send(socket, data) do
            :ok ->
              receive_response(socket, timeout, broker)

            {_, reason} ->
              broker_str = broker_address_str(broker)
              Logger.error("Sending data to broker #{broker_str} failed with #{inspect(reason)}")
              Socket.close(socket)

              {:error, reason}
          end

        response

      {:error, reason} ->
        broker_str = broker_address_str(broker)
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
        broker_str = broker_address_str(broker)
        Logger.error("Receiving data from broker #{broker_str} failed with #{inspect(reason)}")
        Socket.close(socket)

        {:error, reason}
    end
  end

  @impl true
  def format_host(host) do
    case Regex.scan(~r/^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$/, host) do
      [match_data] = [[_, _, _, _, _]] ->
        match_data
        |> tl
        |> List.flatten()
        |> Enum.map(&String.to_integer/1)
        |> List.to_tuple()

      # to_char_list is deprecated from Elixir 1.3 onward
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

  defp build_socket_options([]) do
    [:binary, {:packet, 4}, {:active, false}]
  end

  defp build_socket_options(ssl_options) do
    build_socket_options([]) ++ ssl_options
  end

  defp broker_address_str(broker) do
    "#{inspect(broker.host)}:#{inspect(broker.port)}"
  end

  defp enrich_auth_opts(nil, _host, _port), do: nil

  defp enrich_auth_opts(%AuthConfig{mechanism_opts: opts} = cfg, host, port) do
    %{cfg | mechanism_opts: Map.merge(opts || %{}, %{broker_host: host, broker_port: port})}
  end
end
