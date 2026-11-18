defmodule KafkaEx.TestSupport.FakeTransport do
  @moduledoc """
  Loopback TCP transport selected through the internal `:transport` option; answers ApiVersions v0 and Metadata v0 by API key.
  """
  @behaviour KafkaEx.Network.Behaviour

  require Logger

  alias KafkaEx.Auth.SASL.VersionSupport
  alias KafkaEx.Network.Socket
  alias KafkaEx.Telemetry

  @api_versions 18
  @metadata 3

  @impl true
  def create_socket(host, port, ssl_options \\ [], use_ssl \\ false, auth \\ nil)

  def create_socket(host, port, _ssl_options, use_ssl, auth) do
    pair = open_loopback_pair()
    socket = %Socket{socket: pair.client, ssl: false}

    case validate_auth(auth, %Socket{socket: pair.client, ssl: use_ssl}) do
      :ok ->
        if use_ssl, do: raise_ssl_unsupported(pair)
        remember(pair)
        socket

      {:error, reason} ->
        close_pair(pair)
        Logger.error("SASL authentication failed for #{host}:#{port}: #{inspect(reason)}")
        nil
    end
  end

  @impl true
  def close_socket(_socket), do: :ok

  @impl true
  def close_socket(_broker, nil, _reason), do: :ok

  def close_socket(%{host: host, port: port}, socket, reason) do
    Telemetry.emit_connection_close(host, port, reason)
    close_port(raw_socket(socket))
  end

  @impl true
  def send_async_request(_broker, _data), do: :ok

  @impl true
  def send_sync_request(%{host: host, port: port}, data, _timeout) do
    <<api_key::16-signed, _version::16-signed, correlation_id::32-signed, _::binary>> = IO.iodata_to_binary(data)
    respond(api_key, correlation_id, host, port)
  end

  @impl true
  def format_host(host), do: String.to_charlist(host)

  defp respond(@api_versions, correlation_id, _host, _port),
    do: <<correlation_id::32-signed, 0::16-signed, 2::32-signed, 18::16, 0::16, 0::16, 3::16, 0::16, 0::16>>

  defp respond(@metadata, correlation_id, host, port) do
    IO.iodata_to_binary([
      <<correlation_id::32-signed, 1::32-signed, 1::32-signed>>,
      string(host),
      <<port::32-signed, 0::32-signed>>
    ])
  end

  defp respond(_api_key, _correlation_id, _host, _port), do: {:error, :unsupported_by_fake}

  defp string(value), do: <<byte_size(value)::16-signed, value::binary>>

  defp validate_auth(nil, _socket), do: :ok
  defp validate_auth(auth, socket), do: VersionSupport.validate_config(auth, socket)

  defp raise_ssl_unsupported(pair) do
    close_pair(pair)
    raise ArgumentError, "FakeTransport supports plain TCP only"
  end

  defp open_loopback_pair do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false, ip: {127, 0, 0, 1}])
    {:ok, listen_port} = :inet.port(listener)
    {:ok, client} = :gen_tcp.connect({127, 0, 0, 1}, listen_port, [:binary, active: false])
    {:ok, accepted} = :gen_tcp.accept(listener)
    :ok = :gen_tcp.close(listener)
    %{client: client, accepted: accepted}
  end

  # Ports close with their owner; the pair is found only from the creating process, so a foreign close must fail loudly.
  defp remember(%{client: client} = pair), do: Process.put({__MODULE__, client}, pair)

  defp raw_socket(%Socket{socket: raw}), do: raw
  defp raw_socket(raw), do: raw

  defp close_port(client) do
    case Process.delete({__MODULE__, client}) do
      nil -> close_unowned(client)
      pair -> close_pair(pair)
    end

    :ok
  end

  defp close_unowned(client) do
    case :erlang.port_info(client, :connected) do
      {:connected, owner} when owner != self() ->
        raise ArgumentError, "FakeTransport socket closed from a non-owner process"

      _ ->
        :gen_tcp.close(client)
    end
  end

  defp close_pair(%{client: client, accepted: accepted}) do
    Enum.each([client, accepted], &:gen_tcp.close/1)
  end
end
