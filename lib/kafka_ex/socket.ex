defmodule KafkaEx.Socket do
  @moduledoc """
  This module handle all socket related operations.
  """

  defstruct socket: nil
  @type t :: %KafkaEx.Socket{socket: :gen_tcp.socket}

  @doc """
  Creates a socket.

  For more information about the available options, see `:gen_tcp.connect/3`
  """
  @spec create(string, non_neg_integer, [] | [...]) :: {:ok, KafkaEx.Socket.t} | {:error, any}
  def create(host, port, socket_options \\ []) do
    case :gen_tcp.connect(host, port, socket_options)do
      {:ok, socket} -> {:ok, %KafkaEx.Socket{socket: socket}}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Closes the socket.

  For more information, see `:gen_tcp.close/1`.
  """
  @spec close(KafkaEx.Socket.t) :: :ok
  def close(socket), do: :gen_tcp.close(socket.socket)

  @doc """
  Sends data over the socket.

  For more information, see `:gen_tcp.send/2`.
  """
  @spec send(KafkaEx.Socket.t, iodata) :: :ok | {:error, any}
  def send(socket, data), do: :gen_tcp.send(socket.socket, data)

  @doc """
  Set options to the socket.

  For more information, see :inet.setopts/2`.
  """
  @spec setopts(KafkaEx.Socket.t, list) :: :ok | {:error, any}
  def setopts(socket, options), do: :inet.setopts(socket.socket, options)

  @doc """
  Receives data from the socket.

  For more information, see `:gen_tcp.recv/2`.
  """
  @spec recv(KafkaEx.Socket.t, non_neg_integer) :: {:ok, String.t | binary | term} | {:error, any}
  def recv(socket, length), do: :gen_tcp.recv(socket.socket, length)

  @spec recv(KafkaEx.Socket.t, non_neg_integer, timeout) :: {:ok, String.t | binary | term} | {:error, any}
  def recv(socket, length, timeout), do: :gen_tcp.recv(socket.socket, length, timeout)

  @doc """
  Returns the information about the socket.

  For more information, see `Port.info`
  """
  @spec info(KafkaEx.Socket.t | nil) :: list | nil
  def info(socket) do
    Port.info socket.socket
  end
end
