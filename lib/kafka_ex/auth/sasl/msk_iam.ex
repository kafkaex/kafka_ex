defmodule KafkaEx.Auth.SASL.MskIam do
  @moduledoc """
  SASL/OAUTHBEARER mechanism for AWS MSK IAM authentication.

  Uses AWS SigV4 presigned request as OAUTHBEARER token for MSK clusters
  configured with IAM authentication.

  ## Configuration

      config :kafka_ex,
        brokers: [{"b-1.mycluster.kafka.us-east-1.amazonaws.com", 9098}],
        use_ssl: true,
        sasl: %{
          mechanism: :msk_iam,
          mechanism_opts: %{region: "us-east-1"}
        }

  ## Credentials

  Resolved in order: `credential_provider` function, explicit keys, `aws_credentials` library.
  """

  @behaviour KafkaEx.Auth.Mechanism

  alias KafkaEx.Auth.Config

  @default_version "2020_10_22"
  @service "kafka-cluster"
  @action "kafka-cluster:Connect"
  @user_agent "kafka_ex/elixir"
  @expiry "900"

  @impl true
  def mechanism_name(_config), do: "OAUTHBEARER"

  @impl true
  def authenticate(%Config{mechanism_opts: opts}, send_fun) do
    with {:ok, creds} <- fetch_credentials(opts),
         {:ok, payload} <-
           build_signed_payload(opts[:version] || @default_version, opts.broker_host, opts.region, creds) do
      token = Base.url_encode64(payload, padding: false)

      case send_fun.("n,,\x01auth=Bearer #{token}\x01\x01") do
        {:ok, _} -> :ok
        error -> error
      end
    end
  end

  defp build_signed_payload("2020_10_22" = version, host, region, creds) do
    url = "https://#{host}/?Action=#{URI.encode(@action)}"
    session_token = creds[:session_token]
    sign_opts = if session_token, do: [session_token: session_token], else: []

    signed_url =
      :aws_signature.sign_v4_query_params(
        creds.access_key_id,
        creds.secret_access_key,
        region,
        @service,
        :calendar.universal_time(),
        "GET",
        url,
        sign_opts
      )

    params =
      signed_url
      |> URI.parse()
      |> Map.get(:query)
      |> URI.decode_query()

    payload =
      %{
        "version" => version,
        "host" => host,
        "user-agent" => @user_agent,
        "action" => @action,
        "x-amz-algorithm" => params["X-Amz-Algorithm"],
        "x-amz-credential" => params["X-Amz-Credential"],
        "x-amz-date" => params["X-Amz-Date"],
        "x-amz-signedheaders" => params["X-Amz-SignedHeaders"],
        "x-amz-expires" => @expiry,
        "x-amz-signature" => params["X-Amz-Signature"],
        "x-amz-security-token" => session_token
      }
      |> Enum.reject(fn {_, v} -> is_nil(v) end)
      |> Map.new()
      |> Jason.encode!()

    {:ok, payload}
  end

  defp build_signed_payload(version, _host, _region, _creds) do
    {:error, {:unsupported_version, version}}
  end

  defp fetch_credentials(%{credential_provider: provider}) when is_function(provider, 0) do
    provider.()
  end

  defp fetch_credentials(%{access_key_id: _, secret_access_key: _} = opts) do
    {:ok, Map.take(opts, [:access_key_id, :secret_access_key, :session_token])}
  end

  defp fetch_credentials(_opts) do
    case :aws_credentials.get_credentials() do
      %{access_key_id: ak, secret_access_key: sk} = creds ->
        {:ok,
         %{
           access_key_id: to_string(ak),
           secret_access_key: to_string(sk),
           session_token: if(creds[:token], do: to_string(creds[:token]))
         }}

      _ ->
        {:error, :no_credentials}
    end
  end
end
