defmodule KafkaEx.Auth.SASL.VersionSupport do
  @moduledoc """
  Version compatibility checker for SASL authentication.

  Determines which SASL features are available based on the configured
  Kafka version, preventing runtime errors from unsupported operations.

  ## Support Matrix

  | Kafka Version | SASL/PLAIN | API Versions | SCRAM-SHA |
  |--------------|------------|--------------|------------|
  | 0.8.x        | ❌         | ❌           | ❌        |
  | 0.9.x        | ✅         | ❌           | ❌        |
  | 0.10.0-0.10.1| ✅         | ✅           | ❌        |
  | 0.10.2+      | ✅         | ✅           | ✅        |

  ## Configuration

  Set Kafka version in your config:
  ```elixir
  config :kafka_ex,
    kafka_version: "0.10.2"  # or "kayrock" for latest

  ## Usage
  This module is used internally by the SASL authentication flow
  to make decisions about:

  Whether to fetch API versions during handshake
  Which mechanisms are supported
  Whether SSL is required for PLAIN mechanism
  """

  @type support_level :: :unsupported | :plain_only | :plain_with_api_versions | :full

  @doc """
  Determines SASL support level based on configured Kafka version.
  """
  @spec sasl_support_level() :: support_level()
  # credo:disable-for-next-line Credo.Check.Refactor.CyclomaticComplexity
  def sasl_support_level do
    version = Application.get_env(:kafka_ex, :kafka_version, :default)

    cond do
      version in ["0.8.0", "0.8.2"] -> :unsupported
      is_binary(version) and String.starts_with?(version, "0.8") -> :unsupported
      is_binary(version) and String.starts_with?(version, "0.9") -> :plain_only
      version in ["0.10.0", "0.10.1"] -> :plain_with_api_versions
      is_binary(version) and String.starts_with?(version, "0.10") -> :full
      version == "kayrock" -> :full
      # :default, 0.11+, 1.x, 2.x, etc.
      true -> :full
    end
  end

  @doc """
  Validates SASL configuration against server capabilities.
  Returns :ok or an error tuple with a descriptive message.
  """
  @spec validate_config(KafkaEx.Auth.Config.t(), KafkaEx.Socket.t()) :: :ok | {:error, term()}
  def validate_config(%KafkaEx.Auth.Config{mechanism: mechanism}, socket) do
    case {sasl_support_level(), mechanism} do
      {:unsupported, _} ->
        {:error, :sasl_requires_kafka_0_9_plus}

      {level, mech}
      when level in [:plain_only, :plain_with_api_versions] and
             mech in [:scram_sha_256, :scram_sha_512] ->
        {:error, :scram_requires_kafka_0_10_2_plus}

      {_, :plain} when not socket.ssl ->
        {:error, :plain_requires_tls}

      _ ->
        :ok
    end
  end

  @doc """
  Returns whether to check API versions during SASL handshake.
  """
  @spec check_api_versions?() :: boolean()
  def check_api_versions? do
    sasl_support_level() in [:plain_with_api_versions, :full]
  end
end
