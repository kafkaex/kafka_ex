defmodule KafkaEx.New.Broker do
  @moduledoc false

  alias KafkaEx.Socket

  defstruct node_id: nil,
            host: nil,
            port: nil,
            socket: nil,
            rack: nil,
            socket: nil

  @type t :: %__MODULE__{}

  def put_socket(%__MODULE__{} = broker, socket), do: %{broker | socket: socket}

  def connected?(%__MODULE__{} = broker) do
    broker.socket != nil && Socket.open?(broker.socket)
  end
end
