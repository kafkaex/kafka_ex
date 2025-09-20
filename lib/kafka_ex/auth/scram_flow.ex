defmodule KafkaEx.Auth.ScramFlow do
  @moduledoc """
  SCRAM auth flow (built-in)

  Implements the SCRAM client exchange:
  client-first → server-first → client-final → server-final,
  including nonce generation, salted password derivation, proofs, and server
  signature validation.

  ## Features
    * SCRAM-SHA-256 and SCRAM-SHA-512
    * Stateless helpers for building/validating messages
    * Integrates with `KafkaEx.Auth.SASL` for wire I/O

  ## Security
    * Requires TLS in production deployments
    * Passwords are never logged; be careful with custom logging

  ## See also
    * RFC 5802 / RFC 7677 for SCRAM
    * `KafkaEx.Auth.SASL` – handshake/authenticate transport
    * `KafkaEx.Auth.Config` – username/password provisioning
  """
  alias KafkaEx.Auth.ScramFlow.Internal

  @type algo :: :sha256 | :sha512
  @type send_fun :: (binary() -> {:ok, binary()} | {:error, term()})

  @spec authenticate(binary(), binary(), algo(), send_fun()) :: :ok | {:error, term()}
  def authenticate(username, password, algo, send_fun) do
    st0 = %Internal{algorithm: algo, username: username, password: password, client_nonce: nonce()}
    {client_first, st1} = Internal.client_first(st0)

    with {:ok, server_first} <- send_fun.(client_first),
         {:ok, st2} <- Internal.handle_server_first(st1, server_first),
         {client_final, st3} <- Internal.client_final(st2),
         {:ok, server_final} <- send_fun.(client_final) do
      Internal.verify_server_final(st3, server_final)
    end
  end

  defp nonce(len \\ 24), do: len |> :crypto.strong_rand_bytes() |> Base.encode64(padding: false)
end
