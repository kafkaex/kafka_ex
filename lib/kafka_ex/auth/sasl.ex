defmodule KafkaEx.Auth.SASL do
  @moduledoc """
  SASL authentication orchestrator for KafkaEx.

  Coordinates the SASL authentication flow:
    1. Fetches API versions (if supported by Kafka version)
    2. Performs SASL handshake to negotiate mechanism
    3. Executes mechanism-specific authentication
    4. Manages packet mode switching between raw and length-prefixed

  ## Entry Point

  The `authenticate/2` function is called by `NetworkClient` immediately
  after socket creation when auth configuration is present.

  ## Version Compatibility

    * Kafka 0.9.x - Skips API versions call (not supported)
    * Kafka 0.10.0+ - Queries API versions for optimal protocol selection
    * Kafka 0.10.2+ - Full SCRAM support available

  ## Internal Flow

    1. Check if API versions should be fetched (based on Kafka version)
    2. Send handshake request with desired mechanism
    3. Delegate to mechanism module for authentication exchange
    4. Return `:ok` or `{:error, reason}` to NetworkClient

  ## Compatibility

  Brokers does not tell you the header mode (legacy vs flexible). 
  Some stacks (older IBP, ZK builds, or auth/proxy layers) still map (apiKey=36, ver=2) to header v1 internally,
  so we cap at v1 for authentication request.

  Another option would be to try v2 and fallback to v1 if we meet such broker.
  """

  require Logger

  alias KafkaEx.Auth.Config
  alias KafkaEx.Auth.SASL.CodecBinary
  alias KafkaEx.Network.Socket

  import Bitwise, only: [&&&: 2]

  @max_corr 0x7FFFFFFF

  @mechanisms %{
    plain: KafkaEx.Auth.SASL.Plain,
    scram: KafkaEx.Auth.SASL.Scram,
    oauthbearer: KafkaEx.Auth.SASL.OAuthBearer
    # Future extensions: msk_iam_auth
  }

  # -------- Public API --------

  @spec authenticate(Socket.t(), Config.t()) :: :ok | {:error, term()}
  def authenticate(socket, %Config{} = creds) do
    with {:ok, mech_mod} <- get_mechanism_module(creds),
         api_versions <- fetch_api_versions_if_needed(socket),
         handshake_v <- CodecBinary.pick_handshake_version(api_versions),
         # v2 flexible headers support is limited, better play safe now
         auth_v <- api_versions |> CodecBinary.pick_authenticate_version() |> min(1),
         :ok <- perform_handshake(socket, mech_mod, handshake_v, creds),
         :ok <- mech_mod.authenticate(creds, fn bytes -> send_authenticate(socket, bytes, auth_v) end) do
      Logger.debug("SASL authentication successful")
      :ok
    else
      {:error, reason} = err ->
        Logger.error("SASL authentication failed: #{inspect(reason)}")
        err
    end
  end

  # -------- Internals --------

  defp fetch_api_versions_if_needed(socket) do
    # With Kayrock-based client, always fetch API versions (Kafka 0.10.0+)
    corr = next_correlation_id()
    req = CodecBinary.api_versions_request(corr, 0)

    case send_and_receive(socket, req) do
      {:ok, resp} -> CodecBinary.parse_api_versions_response(resp, corr)
      {:error, _} -> %{}
    end
  end

  defp get_mechanism_module(%Config{mechanism: m}) do
    case Map.get(@mechanisms, m) do
      nil -> {:error, {:unsupported_mechanism, m}}
      mod -> {:ok, mod}
    end
  end

  defp perform_handshake(socket, mech_mod, version, %Config{} = creds) do
    mech = mech_mod.mechanism_name(creds)
    corr = next_correlation_id()
    req = CodecBinary.handshake_request(mech, corr, version)

    with {:ok, resp} <- send_and_receive(socket, req) do
      CodecBinary.parse_handshake_response(resp, corr, mech, version)
    end
  end

  defp send_and_receive(socket, data) do
    with :ok <- Socket.send(socket, data),
         {:ok, payload} <- Socket.recv(socket, 0) do
      {:ok, payload}
    else
      error -> error
    end
  end

  defp send_authenticate(socket, auth_bytes, version) do
    corr = next_correlation_id()
    req = CodecBinary.authenticate_request(auth_bytes, corr, version)

    with {:ok, resp} <- send_and_receive(socket, req) do
      CodecBinary.parse_authenticate_response(resp, corr, version)
    end
  end

  defp next_correlation_id do
    :erlang.unique_integer([:positive, :monotonic]) &&& @max_corr
  end
end
