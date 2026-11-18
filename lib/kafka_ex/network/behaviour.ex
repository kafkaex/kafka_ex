defmodule KafkaEx.Network.Behaviour do
  @moduledoc """
  The transport seam the client calls; `KafkaEx.Network.NetworkClient` is the real transport.

  The client requires `create_socket/5`, `close_socket/3`, `send_sync_request/3` and `send_async_request/2`.
  """

  @type host :: binary
  @type host_port :: non_neg_integer
  @type use_ssl :: boolean
  @type kafka_ex_broker :: KafkaEx.Cluster.Broker.t()
  @type request_data :: iodata
  @type response_data :: iodata

  @type kafka_ex_socket :: KafkaEx.Network.Socket.t()

  @doc """
  Creates a socket to the given host and port.
  """
  @callback create_socket(host, host_port) :: kafka_ex_socket | nil
  @callback create_socket(host, host_port, KafkaEx.ssl_options()) :: kafka_ex_socket | nil
  @callback create_socket(host, host_port, KafkaEx.ssl_options(), use_ssl) :: kafka_ex_socket | nil

  @callback create_socket(host, host_port, KafkaEx.ssl_options(), use_ssl, auth :: KafkaEx.Auth.Config.t() | nil) ::
              kafka_ex_socket | nil

  @doc """
  Close socket, if socket is nil, do nothing.
  """
  @callback close_socket(kafka_ex_socket | nil) :: :ok

  @doc """
  Close socket and emit the connection close telemetry with the given reason.
  """
  @callback close_socket(
              %{host: host, port: host_port} | kafka_ex_broker,
              kafka_ex_socket | port | :ssl.sslsocket() | nil,
              atom
            ) :: :ok

  @doc """
  Send request asynchronously to broker.
  """
  @callback send_async_request(kafka_ex_broker, request_data) :: :ok | {:error, :closed | :inet.posix()}

  @doc """
  Send request synchronously to broker.
  """
  @callback send_sync_request(kafka_ex_broker, iodata, timeout) :: response_data | {:error, any()}

  @doc """
  Returns the host in Erlang IP address format if the host is an IP address.
  Otherwise, returns the host as a char list.
  """
  @callback format_host(binary) :: [char] | :inet.ip_address()

  @optional_callbacks create_socket: 2,
                      create_socket: 3,
                      create_socket: 4,
                      close_socket: 1,
                      close_socket: 3,
                      format_host: 1
end
