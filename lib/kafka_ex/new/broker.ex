defmodule KafkaEx.New.Broker do
  @moduledoc """
  Encapsulates what we know about a broker
  """

  alias KafkaEx.Socket

  defstruct node_id: nil,
            host: nil,
            port: nil,
            socket: nil,
            rack: nil,
            socket: nil

  @type t :: %__MODULE__{}

  @doc false
  def put_socket(%__MODULE__{} = broker, socket), do: %{broker | socket: socket}

  @doc false
  def connected?(%__MODULE__{} = broker) do
    broker.socket != nil && Socket.open?(broker.socket)
  end
end
