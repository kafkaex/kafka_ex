defmodule KafkaEx.Socket do
  @moduledoc """
  This module handle all socket related operations.
  """

  defstruct socket: nil, ssl: false

  @type t :: %KafkaEx.Socket{
          socket: :gen_tcp.socket() | :ssl.sslsocket(),
          ssl: boolean
        }

  @doc """
  Creates a socket.

  For more information about the available options, see `:ssl.connect/3` for ssl
  or `:gen_tcp.connect/3` for non ssl.
  """
  @spec create(:inet.ip_address(), non_neg_integer, [] | [...]) ::
          {:ok, KafkaEx.Socket.t()} | {:error, any}
  def create(host, port, socket_options \\ [], is_ssl \\ false) do
    case create_socket(host, port, is_ssl, socket_options) do
      {:ok, socket} -> {:ok, %KafkaEx.Socket{socket: socket, ssl: is_ssl}}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Closes the socket.

  For more information, see `:ssl.close/1` for ssl or `:gen_tcp.send/1` for non ssl.
  """
  @spec close(KafkaEx.Socket.t()) :: :ok
  def close(%KafkaEx.Socket{ssl: true} = socket), do: :ssl.close(socket.socket)
  def close(socket), do: :gen_tcp.close(socket.socket)

  @doc """
  Sends data over the socket.

  It handles both, SSL and non SSL sockets.

  For more information, see `:ssl.send/2` for ssl or `:gen_tcp.send/2` for non ssl.
  """
  @spec send(KafkaEx.Socket.t(), iodata) :: :ok | {:error, any}
  def send(%KafkaEx.Socket{ssl: true} = socket, data) do
    :ssl.send(socket.socket, data)
  end

  def send(socket, data) do
    :gen_tcp.send(socket.socket, data)
  end

  @doc """
  Set options to the socket.

  For more information, see `:ssl.setopts/2` for ssl or `:inet.setopts/2` for non ssl.
  """
  @spec setopts(KafkaEx.Socket.t(), list) :: :ok | {:error, any}
  def setopts(%KafkaEx.Socket{ssl: true} = socket, options) do
    :ssl.setopts(socket.socket, options)
  end

  def setopts(socket, options) do
    :inet.setopts(socket.socket, options)
  end

  @doc """
  Receives data from the socket.

  For more information, see `:ssl.recv/2` for ssl or `:gen_tcp.recv/2` for non ssl.
  """
  @spec recv(KafkaEx.Socket.t(), non_neg_integer) ::
          {:ok, String.t() | binary | term} | {:error, any}
  def recv(%KafkaEx.Socket{ssl: true} = socket, length) do
    :ssl.recv(socket.socket, length)
  end

  def recv(socket, length) do
    :gen_tcp.recv(socket.socket, length)
  end

  @spec recv(KafkaEx.Socket.t(), non_neg_integer, timeout) ::
          {:ok, String.t() | binary | term} | {:error, any}
  def recv(%KafkaEx.Socket{ssl: true} = socket, length, timeout) do
    :ssl.recv(socket.socket, length, timeout)
  end

  def recv(socket, length, timeout) do
    :gen_tcp.recv(socket.socket, length, timeout)
  end

  @doc """
  Returns true if the socket is open
  """
  @spec open?(KafkaEx.Socket.t()) :: boolean
  def open?(%KafkaEx.Socket{} = socket) do
    info(socket) != nil
  end

  @doc """
  Returns the information about the socket.

  For more information, see `Port.info`
  """
  @spec info(KafkaEx.Socket.t()) :: list | nil
  def info(socket) do
    socket
    |> extract_port
    |> Port.info()
  end

  defp extract_port(%KafkaEx.Socket{ssl: true} = socket) do
    {:sslsocket, {:gen_tcp, port, _, _}, _} = socket.socket
    port
  end

  defp extract_port(socket), do: socket.socket

  defp create_socket(host, port, true, socket_options) do
    :ssl.connect(host, port, socket_options)
  end

  defp create_socket(host, port, false, socket_options) do
    :gen_tcp.connect(host, port, socket_options)
  end
end
