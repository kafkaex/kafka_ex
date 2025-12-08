defmodule KafkaEx.Auth.SASL.OAuthBearer do
  @moduledoc """
  SASL/OAUTHBEARER mechanism for Kafka (KIP-255, KIP-342).

  ## Configuration

      KafkaEx.Auth.Config.new(%{
        mechanism: :oauthbearer,
        mechanism_opts: %{
          token_provider: fn -> {:ok, "jwt-token"} end,
          extensions: %{"traceId" => "abc123"}  # optional, KIP-342
        }
      })

  ## Token Provider

  The `token_provider` is a 0-arity function returning:
  - `{:ok, token}` - JWT token string
  - `{:error, reason}` - failure

  The provider is called on each new connection and is responsible for
  caching and refreshing tokens.
  """

  @behaviour KafkaEx.Auth.Mechanism

  alias KafkaEx.Auth.Config

  @impl true
  def mechanism_name(_config), do: "OAUTHBEARER"

  @impl true
  def authenticate(%Config{mechanism_opts: opts}, send_fun) do
    with {:ok, token} <- fetch_token(opts) do
      case send_fun.(build_initial_response(token, opts)) do
        {:ok, _} -> :ok
        {:error, _} = err -> err
      end
    end
  end

  defp fetch_token(%{token_provider: provider}) when is_function(provider, 0) do
    case provider.() do
      {:ok, token} when is_binary(token) and byte_size(token) > 0 -> {:ok, token}
      {:ok, _} -> {:error, :invalid_token}
      {:error, _} = err -> err
    end
  end

  defp fetch_token(_), do: {:error, :missing_token_provider}

  # RFC 7628 / KIP-255: "n,,\x01auth=Bearer <token>[\x01ext=val...]\x01\x01"
  defp build_initial_response(token, opts) do
    extensions = Map.get(opts, :extensions, %{})
    "n,,\x01auth=Bearer #{token}#{encode_extensions(extensions)}\x01\x01"
  end

  defp encode_extensions(exts) when map_size(exts) == 0, do: ""

  defp encode_extensions(exts) do
    Enum.map_join(exts, "", fn {k, v} -> "\x01#{k}=#{v}" end)
  end
end
