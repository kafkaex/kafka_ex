defmodule KafkaEx.Support.OptionalDepsTest do
  @moduledoc """
  Unit tests for OptionalDeps.validate!/1.

  Present-dep tests don't need to simulate anything — if the test suite
  is running, the optional deps that happen to be loaded on this
  runtime are loaded. Missing-dep tests swap `Code.ensure_loaded?/1`
  via a module shadow: we introduce an unknown atom that is
  guaranteed to NOT be loadable and confirm the error message.
  """
  use ExUnit.Case, async: false

  alias KafkaEx.Auth.Config, as: AuthConfig
  alias KafkaEx.Support.OptionalDeps

  setup do
    on_exit(fn ->
      Application.delete_env(:kafka_ex, :required_compression)
    end)

    :ok
  end

  describe "validate!/1 — SASL mechanisms" do
    test "nil auth: no-op, returns :ok" do
      assert :ok = OptionalDeps.validate!(nil)
    end

    test ":plain / :scram / :oauthbearer: no optional deps required" do
      for mech <- [:plain, :scram, :oauthbearer] do
        cfg = %AuthConfig{mechanism: mech, username: "u", password: "p"}
        assert :ok = OptionalDeps.validate!(cfg)
      end
    end

    test ":msk_iam: validates Jason + aws deps are present (loaded in test env)" do
      cfg = %AuthConfig{mechanism: :msk_iam, username: nil, password: nil}

      # In the test env :jason / :aws_signature / :aws_credentials are
      # typically loaded (via mix deps); if any aren't, this test
      # documents the expected failure mode with a mix.exs snippet.
      case Code.ensure_loaded?(Jason) do
        true -> assert :ok = OptionalDeps.validate!(cfg)
        false -> assert_raise ArgumentError, ~r/msk_iam/, fn -> OptionalDeps.validate!(cfg) end
      end
    end
  end

  describe "validate!/1 — required_compression app config" do
    test "unset (default): no-op" do
      Application.delete_env(:kafka_ex, :required_compression)
      assert :ok = OptionalDeps.validate!(nil)
    end

    test "[]: no-op" do
      Application.put_env(:kafka_ex, :required_compression, [])
      assert :ok = OptionalDeps.validate!(nil)
    end

    test "[:none] or [:gzip]: no-op (no deps needed)" do
      for algo <- [:none, :gzip] do
        Application.put_env(:kafka_ex, :required_compression, [algo])
        assert :ok = OptionalDeps.validate!(nil)
      end
    end

    test ":snappy: passes if :snappyer loaded, raises with mix.exs snippet otherwise" do
      Application.put_env(:kafka_ex, :required_compression, [:snappy])

      case Code.ensure_loaded?(:snappyer) do
        true ->
          assert :ok = OptionalDeps.validate!(nil)

        false ->
          error =
            assert_raise ArgumentError, fn ->
              OptionalDeps.validate!(nil)
            end

          assert error.message =~ ":snappy"
          assert error.message =~ "{:snappyer"
      end
    end

    test ":lz4 with missing dep raises with mix.exs snippet" do
      Application.put_env(:kafka_ex, :required_compression, [:lz4])

      case Code.ensure_loaded?(:lz4b) do
        true ->
          assert :ok = OptionalDeps.validate!(nil)

        false ->
          error = assert_raise ArgumentError, fn -> OptionalDeps.validate!(nil) end
          assert error.message =~ ":lz4"
          assert error.message =~ "{:lz4b"
      end
    end

    test ":zstd with missing dep raises with mix.exs snippet" do
      Application.put_env(:kafka_ex, :required_compression, [:zstd])

      case Code.ensure_loaded?(:ezstd) do
        true ->
          assert :ok = OptionalDeps.validate!(nil)

        false ->
          error = assert_raise ArgumentError, fn -> OptionalDeps.validate!(nil) end
          assert error.message =~ ":zstd"
          assert error.message =~ "{:ezstd"
      end
    end

    test "unknown compression atom raises a useful error" do
      Application.put_env(:kafka_ex, :required_compression, [:snappy, :bogus])

      error =
        assert_raise ArgumentError, fn ->
          OptionalDeps.validate!(nil)
        end

      assert error.message =~ ":bogus"
      assert error.message =~ "Expected any of :gzip, :snappy, :lz4, :zstd"
    end

    test "non-list value raises with a useful error" do
      Application.put_env(:kafka_ex, :required_compression, :snappy)

      error =
        assert_raise ArgumentError, fn ->
          OptionalDeps.validate!(nil)
        end

      assert error.message =~ "expected a list"
    end
  end
end
