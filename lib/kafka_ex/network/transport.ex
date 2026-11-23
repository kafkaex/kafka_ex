defmodule KafkaEx.Network.Transport do
  @moduledoc false

  alias KafkaEx.Network.Socket

  @type address :: %{host: binary(), port: :inet.port_number(), role: term()}

  @callback connect(address(), socket_options :: Keyword.t(), use_ssl :: boolean(), timeout()) ::
              {:ok, Socket.t()} | {:error, term()}
  @callback write(Socket.t(), iodata()) :: :ok | {:error, term()}
  @callback read(Socket.t(), timeout()) :: {:ok, binary()} | {:error, term()}
  @callback activate(Socket.t()) :: :ok | {:error, term()}
  @callback close(Socket.t()) :: :ok
end
