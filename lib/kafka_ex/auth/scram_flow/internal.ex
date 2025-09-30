defmodule KafkaEx.Auth.ScramFlow.Internal do
  @moduledoc false

  # Internal implementation of RFC 5802/7677 SCRAM authentication
  #
  # ## Implementation Notes
  # 
  # This module implements the SCRAM client exchange following RFC 5802 (SCRAM)
  # and RFC 7677 (SCRAM-SHA-256). The flow consists of:
  # 
  # 1. Client First: Send username and client nonce
  #    Format: "n,,n=<username>,r=<nonce>"
  # 
  # 2. Server First: Receive salt, iterations, combined nonce
  #    Parse: "r=<nonce>,s=<salt>,i=<iterations>"
  # 
  # 3. Client Final: Compute and send proof
  #    - Derive salted password using PBKDF2
  #    - Calculate client/server keys
  #    - Generate auth message and signatures
  #    Format: "c=<channel>,r=<nonce>,p=<proof>"
  # 
  # 4. Server Final: Verify server signature
  #    Verify: "v=<server_signature>"
  #
  # ## Key Security Properties
  # - Client proof demonstrates password knowledge without sending it
  # - Server signature proves the server also knows the password
  # - Channel binding (though we use "n,," for no binding)
  # - Nonces prevent replay attacks
  #
  # ## Special Character Escaping
  # Username escaping follows SCRAM spec:
  # - "=" becomes "=3D"
  # - "," becomes "=2C"

  defstruct [
    :algorithm, :username, :password,
    :client_nonce, :client_first_bare,
    :server_first_raw, :server_nonce, :salt, :iterations,
    :auth_message, :server_signature
  ]

  @type t :: %__MODULE__{}
  @type algo :: :sha256 | :sha512

  @spec client_first(%__MODULE__{}) :: {binary(), %__MODULE__{}}
  def client_first(%__MODULE__{} = s) do
    gs2 = "n,,"
    cfb = "n=#{escape(s.username)},r=#{s.client_nonce}"
    {gs2 <> cfb, %{s | client_first_bare: cfb}}
  end

  @spec handle_server_first(%__MODULE__{}, binary()) :: {:ok, %__MODULE__{}} | {:error, term()}
  def handle_server_first(%__MODULE__{} = s, server_first) do
    with %{"r" => nonce, "s" => salt_b64, "i" => iter_str} <- parse_kv(server_first),
         true <- String.starts_with?(nonce, s.client_nonce) or {:error, :invalid_server_nonce} do
      {:ok,
       %{
         s
         | server_first_raw: server_first,
           server_nonce: nonce,
           salt: Base.decode64!(salt_b64),
           iterations: String.to_integer(iter_str)
       }}
    else
      %{} -> {:error, :invalid_server_first_message}
      {:error, _} = e -> e
    end
  end

  @spec client_final(%__MODULE__{}) :: {binary(), %__MODULE__{}}
  def client_final(%__MODULE__{} = s) do
    cb64 = Base.encode64("n,,")               # "biws"
    cfwp = "c=#{cb64},r=#{s.server_nonce}"    # client-final-without-proof

    auth = s.client_first_bare <> "," <> s.server_first_raw <> "," <> cfwp
    {proof, server_sig} = proof_and_server_sig(s, auth)

    {cfwp <> ",p=" <> Base.encode64(proof), %{s | auth_message: auth, server_signature: server_sig}}
  end

  @spec verify_server_final(%__MODULE__{}, binary()) :: :ok | {:error, term()}
  def verify_server_final(%__MODULE__{} = s, server_final) do
    case parse_kv(server_final) do
      %{"v" => v_b64} ->
        if v_b64 == Base.encode64(s.server_signature), do: :ok, else: {:error, :invalid_server_signature}

      %{"e" => err} ->
        {:error, {:server_error, err}}

      _ ->
        {:error, :invalid_server_final_message}
    end
  end

  # -------- Helpers

  defp proof_and_server_sig(%__MODULE__{algorithm: algo} = s, auth) do
    {hash, dklen} = case algo do
      :sha256 -> {:sha256, 32}
      :sha512 -> {:sha512, 64}
    end

    salted = :crypto.pbkdf2_hmac(hash, s.password, s.salt, s.iterations, dklen)
    client_key = :crypto.mac(:hmac, hash, salted, "Client Key")
    stored_key = :crypto.hash(hash, client_key)
    client_sig = :crypto.mac(:hmac, hash, stored_key, auth)
    proof      = :crypto.exor(client_key, client_sig)

    server_key = :crypto.mac(:hmac, hash, salted, "Server Key")
    server_sig = :crypto.mac(:hmac, hash, server_key, auth)

    {proof, server_sig}
  end

  defp parse_kv(message) do
    message
    |> String.split(",", trim: true)
    |> Enum.map(&String.split(&1, "=", parts: 2))
    |> Enum.filter(&match?([_, _], &1))
    |> Map.new(fn [k, v] -> {k, v} end)
  end

  defp escape(username),
    do: username |> String.replace("=", "=3D") |> String.replace(",", "=2C")
end
