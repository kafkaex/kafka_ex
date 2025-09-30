defmodule KafkaEx.Auth.Mechanism do
  @moduledoc """
  Behaviour for SASL authentication mechanisms.

  Each mechanism must implement:
    * `mechanism_name/1` - Returns the mechanism name for SASL handshake
    * `authenticate/2` - Performs the authentication exchange

  ## Built-in Implementations

    * `KafkaEx.Auth.SASL.Plain` - Simple username/password (requires TLS)
    * `KafkaEx.Auth.SASL.Scram` - Challenge-response with SHA-256/512

  ## Extending Authentication

  To implement a custom mechanism:
  
  defmodule MyAuth do
    @behaviour KafkaEx.Auth.Mechanism
    
    @impl true
    def mechanism_name(_config), do: "OAUTHBEARER"
    
    @impl true
    def authenticate(config, send_fun) do
      # Exchange authentication messages with broker
      # using send_fun to send and receive data
      :ok
    end
  end
  """
  
  @type auth_opts :: KafkaEx.Auth.Config.t()
  @type send_fun :: (binary() -> {:ok, binary() | nil} | {:error, term()})
  
  @doc """
  Returns mechanism name for handshake.
  """
  @callback mechanism_name(auth_opts()) :: String.t()
  
  @doc """
  Performs the authentication exchange after handshake.
  """
  @callback authenticate(auth_opts(), send_fun()) ::
    :ok | {:error, term()}
end