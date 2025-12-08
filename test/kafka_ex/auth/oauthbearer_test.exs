defmodule KafkaEx.Auth.SASL.OAuthBearerTest do
  use ExUnit.Case, async: true

  @moduletag :sasl

  alias KafkaEx.Auth.Config
  alias KafkaEx.Auth.SASL.OAuthBearer

  describe "mechanism_name/1" do
    test "returns OAUTHBEARER" do
      cfg = build_config(fn -> {:ok, "t"} end)
      assert OAuthBearer.mechanism_name(cfg) == "OAUTHBEARER"
    end
  end

  describe "authenticate/2" do
    test "sends correct initial response" do
      cfg = build_config(fn -> {:ok, "my-jwt"} end)

      send_fn = fn payload ->
        assert payload == "n,,\x01auth=Bearer my-jwt\x01\x01"
        {:ok, nil}
      end

      assert OAuthBearer.authenticate(cfg, send_fn) == :ok
    end

    test "includes extensions (KIP-342)" do
      cfg = build_config(fn -> {:ok, "tok"} end, extensions: %{"traceId" => "123"})

      send_fn = fn payload ->
        assert payload =~ "auth=Bearer tok"
        assert payload =~ "\x01traceId=123"
        {:ok, nil}
      end

      assert OAuthBearer.authenticate(cfg, send_fn) == :ok
    end

    test "returns error from token_provider" do
      cfg = build_config(fn -> {:error, :expired} end)
      assert OAuthBearer.authenticate(cfg, fn _ -> {:ok, nil} end) == {:error, :expired}
    end

    test "returns error for empty token" do
      cfg = build_config(fn -> {:ok, ""} end)
      assert OAuthBearer.authenticate(cfg, fn _ -> {:ok, nil} end) == {:error, :invalid_token}
    end

    test "returns error when token_provider missing" do
      cfg = %Config{mechanism: :oauthbearer, username: nil, password: nil, mechanism_opts: %{}}
      assert OAuthBearer.authenticate(cfg, fn _ -> {:ok, nil} end) == {:error, :missing_token_provider}
    end

    test "propagates send_fun errors" do
      cfg = build_config(fn -> {:ok, "tok"} end)
      send_fn = fn _ -> {:error, {:auth_failed, :sasl_authentication_failed}} end
      assert OAuthBearer.authenticate(cfg, send_fn) == {:error, {:auth_failed, :sasl_authentication_failed}}
    end
  end

  defp build_config(provider, extra \\ []) do
    %Config{
      mechanism: :oauthbearer,
      username: nil,
      password: nil,
      mechanism_opts: Map.new([{:token_provider, provider} | extra])
    }
  end
end
