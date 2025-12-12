defmodule KafkaEx.Auth.ConfigOAuthBearerTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Auth.Config

  describe "new/1 with oauthbearer" do
    test "builds config with token_provider" do
      cfg =
        Config.new(%{
          mechanism: :oauthbearer,
          mechanism_opts: %{token_provider: fn -> {:ok, "t"} end}
        })

      assert cfg.mechanism == :oauthbearer
      assert is_nil(cfg.username)
      assert is_nil(cfg.password)
    end

    test "accepts extensions" do
      cfg =
        Config.new(%{
          mechanism: :oauthbearer,
          mechanism_opts: %{
            token_provider: fn -> {:ok, "t"} end,
            extensions: %{"foo" => "bar"}
          }
        })

      assert cfg.mechanism_opts.extensions == %{"foo" => "bar"}
    end

    test "raises without token_provider" do
      assert_raise ArgumentError, ~r/token_provider/, fn ->
        Config.new(%{mechanism: :oauthbearer, mechanism_opts: %{}})
      end
    end

    test "raises when token_provider is not a function" do
      assert_raise ArgumentError, ~r/token_provider/, fn ->
        Config.new(%{mechanism: :oauthbearer, mechanism_opts: %{token_provider: "not-a-fn"}})
      end
    end

    test "raises on reserved 'auth' extension" do
      assert_raise ArgumentError, ~r/reserved/, fn ->
        Config.new(%{
          mechanism: :oauthbearer,
          mechanism_opts: %{
            token_provider: fn -> {:ok, "t"} end,
            extensions: %{"auth" => "bad"}
          }
        })
      end
    end
  end
end
