defmodule KafkaEx.New.Structs.Broker do
  @moduledoc """
  Encapsulates what we know about a broker and our connection
  """

  alias KafkaEx.Socket

  defstruct node_id: nil,
            host: nil,
            port: nil,
            socket: nil,
            rack: nil

  @type t :: %__MODULE__{
          node_id: non_neg_integer,
          host: binary,
          port: non_neg_integer,
          socket: Socket.t() | nil,
          rack: binary
        }

  @doc false
  def put_socket(%__MODULE__{} = broker, socket), do: %{broker | socket: socket}

  @doc false
  def connected?(%__MODULE__{} = broker) do
    broker.socket != nil && Socket.open?(broker.socket)
  end

  def has_socket?(%__MODULE__{socket: %Socket{socket: socket}}, socket),
    do: true

  def has_socket?(_, _), do: false
end
