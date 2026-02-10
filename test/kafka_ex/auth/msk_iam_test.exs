defmodule KafkaEx.Auth.SASL.MskIamTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Auth.Config
  alias KafkaEx.Auth.SASL.MskIam

  @moduletag :sasl
  describe "mechanism_name/1" do
    test "returns OAUTHBEARER" do
      assert MskIam.mechanism_name(build_config()) == "OAUTHBEARER"
    end
  end

  describe "authenticate/2" do
    test "sends correctly formatted initial response" do
      cfg = build_config()

      send_fn = fn payload ->
        build_config()
        assert String.starts_with?(payload, "n,,\x01auth=Bearer ")
        assert String.ends_with?(payload, "\x01\x01")

        decoded = decode_payload(payload)

        assert decoded["version"] == "2020_10_22"
        assert decoded["host"] == "b-1.test.kafka.us-east-1.amazonaws.com"
        assert decoded["action"] == "kafka-cluster:Connect"
        assert decoded["x-amz-algorithm"] == "AWS4-HMAC-SHA256"
        assert decoded["x-amz-signedheaders"] == "host"
        assert decoded["x-amz-expires"] == "900"
        assert is_binary(decoded["x-amz-credential"])
        assert is_binary(decoded["x-amz-date"])
        assert is_binary(decoded["x-amz-signature"])
        refute Map.has_key?(decoded, "x-amz-security-token")

        {:ok, nil}
      end

      assert MskIam.authenticate(cfg, send_fn) == :ok
    end

    test "includes session token when provided" do
      cfg = build_config(session_token: "my-session-token")

      send_fn = fn payload ->
        assert decode_payload(payload)["x-amz-security-token"] == "my-session-token"
        {:ok, nil}
      end

      assert MskIam.authenticate(cfg, send_fn) == :ok
    end

    test "uses credential_provider" do
      provider = fn ->
        {:ok,
         %{
           access_key_id: "PROVIDER_KEY",
           secret_access_key: "PROVIDER_SECRET",
           session_token: "PROVIDER_TOKEN"
         }}
      end

      cfg = build_config(credential_provider: provider, access_key_id: nil, secret_access_key: nil)

      send_fn = fn payload ->
        decoded = decode_payload(payload)
        assert decoded["x-amz-credential"] =~ "PROVIDER_KEY"
        assert decoded["x-amz-security-token"] == "PROVIDER_TOKEN"
        {:ok, nil}
      end

      assert MskIam.authenticate(cfg, send_fn) == :ok
    end

    test "supports custom version" do
      cfg = build_config(version: "2020_10_22")

      send_fn = fn payload ->
        assert decode_payload(payload)["version"] == "2020_10_22"
        {:ok, nil}
      end

      assert MskIam.authenticate(cfg, send_fn) == :ok
    end

    test "propagates send_fun error" do
      cfg = build_config()
      assert MskIam.authenticate(cfg, fn _ -> {:error, :auth_failed} end) == {:error, :auth_failed}
    end

    test "propagates credential_provider error" do
      cfg = build_config(credential_provider: fn -> {:error, :expired} end, access_key_id: nil)
      assert MskIam.authenticate(cfg, fn _ -> {:ok, nil} end) == {:error, :expired}
    end

    test "returns error for unsupported version" do
      cfg = build_config(version: "2099_01_01")
      assert MskIam.authenticate(cfg, fn _ -> {:ok, nil} end) == {:error, {:unsupported_version, "2099_01_01"}}
    end
  end

  defp build_config(overrides \\ []) do
    opts =
      [
        broker_host: "b-1.test.kafka.us-east-1.amazonaws.com",
        broker_port: 9098,
        region: "us-east-1",
        access_key_id: "AKIATEST123456789012",
        secret_access_key: "TestSecretKey1234567890123456789012345"
      ]
      |> Keyword.merge(overrides)
      |> Map.new()

    %Config{mechanism: :msk_iam, username: nil, password: nil, mechanism_opts: opts}
  end

  defp decode_payload(payload) do
    payload
    |> String.trim_leading("n,,\x01auth=Bearer ")
    |> String.trim_trailing("\x01\x01")
    |> Base.url_decode64!(padding: false)
    |> Jason.decode!()
  end
end
