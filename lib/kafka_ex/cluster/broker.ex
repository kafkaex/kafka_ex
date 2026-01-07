defmodule KafkaEx.Cluster.Broker do
  @moduledoc """
  Encapsulates what we know about a broker and our connection.

  A broker represents a Kafka node in the cluster with its connection state.
  """

  alias KafkaEx.Network.Socket

  defstruct node_id: nil, host: nil, port: nil, socket: nil, rack: nil

  @type t :: %__MODULE__{
          node_id: non_neg_integer,
          host: binary,
          port: non_neg_integer,
          socket: Socket.t() | nil,
          rack: binary
        }

  @doc """
  Returns a human-readable address string for the broker.

  ## Examples

      iex> broker = %KafkaEx.Cluster.Broker{host: "localhost", port: 9092, node_id: 1}
      iex> KafkaEx.Cluster.Broker.address(broker)
      "localhost:9092"
  """
  @spec address(t()) :: String.t()
  def address(%__MODULE__{host: host, port: port}), do: "#{host}:#{port}"

  @doc """
  Returns a string identifying the broker with node_id and address.

  ## Examples

      iex> broker = %KafkaEx.Cluster.Broker{host: "localhost", port: 9092, node_id: 1}
      iex> KafkaEx.Cluster.Broker.to_string(broker)
      "broker 1 (localhost:9092)"
  """
  @spec to_string(t()) :: String.t()
  def to_string(%__MODULE__{node_id: node_id} = broker), do: "broker #{node_id} (#{address(broker)})"

  @doc """
  Checks if the broker has an active socket connection.
  Returns `true` if the broker has a non-nil socket that is currently open, `false` otherwise.
  """
  @spec connected?(t()) :: boolean()
  def connected?(%__MODULE__{socket: nil}), do: false
  def connected?(%__MODULE__{socket: socket}), do: Socket.open?(socket)

  @doc """
  Checks if the broker has a socket (connected or not).
  """
  @spec has_socket?(t()) :: boolean()
  def has_socket?(%__MODULE__{socket: nil}), do: false
  def has_socket?(%__MODULE__{socket: _}), do: true

  @doc """
  Checks if the broker's socket matches the given socket.
  """
  @spec has_socket?(t(), Socket.t()) :: boolean()
  def has_socket?(%__MODULE__{socket: %Socket{socket: socket}}, socket), do: true
  def has_socket?(_, _), do: false

  @doc """
  Updates the broker's socket.
  """
  @spec put_socket(t(), Socket.t() | nil) :: t()
  def put_socket(%__MODULE__{} = broker, socket), do: %{broker | socket: socket}
end
