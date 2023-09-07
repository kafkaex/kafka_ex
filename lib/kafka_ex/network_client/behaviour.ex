defmodule KafkaEx.NetworkClient.Behaviour do
  @moduledoc """
  Behaviour for any network client.
  Created mainly to allow mocking request & responses in unit tests.
  """

  @type host :: binary
  @type host_port :: non_neg_integer
  @type use_ssl :: boolean
  @type kafka_ex_broker ::
          KafkaEx.Protocol.Metadata.Broker.t() | KafkaEx.New.Structs.Broker.t()
  @type request_data :: iodata
  @type response_data :: iodata

  # Dirty hack to allow mocking of socket in unit tests for elixir 1.8
  # We should remove this when we drop support for elixir 1.8
  if Version.match?(System.version(), ">= 1.10.0") do
    @type kafka_ex_socket :: KafkaEx.Socket.t()
  else
    @type kafka_ex_socket :: any
  end

  @doc """
  Creates a socket to the given host and port.
  """
  @callback create_socket(host, host_port) :: kafka_ex_socket | nil
  @callback create_socket(host, host_port, KafkaEx.ssl_options()) ::
              kafka_ex_socket | nil
  @callback create_socket(host, host_port, KafkaEx.ssl_options(), use_ssl) ::
              kafka_ex_socket | nil

  @doc """
  Close socket, if socket is nil, do nothing.
  """
  @callback close_socket(kafka_ex_socket | nil) :: :ok

  @doc """
  Send request asynchronously to broker.
  """
  @callback send_async_request(kafka_ex_broker, request_data) ::
              :ok | {:error, :closed | :inet.posix()}

  @doc """
  Send request synchronously to broker.
  """
  @callback send_sync_request(kafka_ex_broker, iodata, timeout) ::
              response_data | {:error, any()}

  @doc """
  Returns the host in Erlang IP address format if the host is an IP address.
  Otherwise, returns the host as a char list.
  """
  @callback format_host(binary) :: [char] | :inet.ip_address()
end
