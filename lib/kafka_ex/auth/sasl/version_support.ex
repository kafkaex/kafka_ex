defmodule KafkaEx.Auth.SASL.VersionSupport do
  @moduledoc """
  Version compatibility checker for SASL authentication.

  With the Kayrock-based client, API version negotiation is handled automatically.
  This module provides SASL validation to ensure proper configuration.

  ## Support Matrix

  | Feature        | Support |
  |---------------|---------|
  | SASL/PLAIN    | ✅      |
  | API Versions  | ✅      |
  | SCRAM-SHA     | ✅      |

  Note: SASL/PLAIN requires SSL/TLS for security.
  """

  @type support_level :: :full

  @doc """
  Returns the SASL support level.

  With the Kayrock-based client, full SASL support is available.
  API version negotiation is handled automatically by the client.
  """
  @spec sasl_support_level() :: support_level()
  def sasl_support_level do
    :full
  end

  @doc """
  Validates SASL configuration against server capabilities.
  Returns :ok or an error tuple with a descriptive message.

  Currently validates:
  - PLAIN mechanism requires TLS for security
  """
  @spec validate_config(KafkaEx.Auth.Config.t(), KafkaEx.Socket.t()) :: :ok | {:error, term()}
  def validate_config(%KafkaEx.Auth.Config{mechanism: :plain}, socket) when not socket.ssl do
    {:error, :plain_requires_tls}
  end

  def validate_config(%KafkaEx.Auth.Config{}, _socket) do
    :ok
  end

  @doc """
  Returns whether to check API versions during SASL handshake.

  Always returns true with the Kayrock-based client.
  """
  @spec check_api_versions?() :: boolean()
  def check_api_versions? do
    true
  end
end
