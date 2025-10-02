defmodule KafkaEx.NetworkClient do
  @moduledoc """
  KafkaEx implementation of Client used to connect to Kafka Broker
  """
  @behaviour KafkaEx.NetworkClient.Behaviour

  require Logger
  alias KafkaEx.Socket
  alias KafkaEx.Auth.SASL.VersionSupport
  alias KafkaEx.Auth.Config, as: AuthConfig

  @impl true
  def create_socket(host, port, ssl_options \\ [], use_ssl \\ false, auth_opts \\ nil) do
    case Socket.create(format_host(host), port, build_socket_options(ssl_options), use_ssl) do
      {:ok, socket} ->
        case maybe_authenticate_sasl(socket, auth_opts) do
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
        Logger.log(
          :error,
          "Asynchronously sending data to broker #{inspect(broker.host)}:#{inspect(broker.port)} failed with #{inspect(reason)}"
        )

        reason
    end
  end

  @impl true
  def send_sync_request(%{:socket => socket} = broker, data, timeout) do
    :ok = Socket.setopts(socket, [:binary, {:packet, 4}, {:active, false}])

    response =
      case Socket.send(socket, data) do
        :ok ->
          case Socket.recv(socket, 0, timeout) do
            {:ok, data} ->
              :ok = Socket.setopts(socket, [:binary, {:packet, 4}, {:active, true}])

              data

            {:error, reason} ->
              Logger.log(
                :error,
                "Receiving data from broker #{inspect(broker.host)}:#{inspect(broker.port)} failed with #{inspect(reason)}"
              )

              Socket.close(socket)

              {:error, reason}
          end

        {_, reason} ->
          Logger.log(
            :error,
            "Sending data to broker #{inspect(broker.host)}:#{inspect(broker.port)} failed with #{inspect(reason)}"
          )

          Socket.close(socket)

          {:error, reason}
      end

    response
  end

  def send_sync_request(nil, _, _) do
    {:error, :no_broker}
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
        apply(String, :to_char_list, [host])
    end
  end

  defp maybe_authenticate_sasl(_socket, nil), do: :ok

  defp maybe_authenticate_sasl(socket, %AuthConfig{} = cfg) do
    case VersionSupport.validate_config(cfg, socket) do
      :ok -> KafkaEx.Auth.SASL.authenticate(socket, cfg)
      {:error, reason} -> {:error, reason}
    end
  end

  defp build_socket_options([]) do
    [:binary, {:packet, 4}, {:active, false}]
  end

  defp build_socket_options(ssl_options) do
    build_socket_options([]) ++ ssl_options
  end
end
