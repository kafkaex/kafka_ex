defmodule KafkaEx.Auth.SASL.ScramTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Auth.SASL.Scram
  alias KafkaEx.Auth.Config

  describe "mechanism_name/1" do
    test "returns SCRAM-SHA-256 by default" do
      config = %Config{mechanism: :scram, username: "user", password: "pass"}
      assert Scram.mechanism_name(config) == "SCRAM-SHA-256"
    end

    test "returns SCRAM-SHA-256 with explicit sha256 algo" do
      config = %Config{
        mechanism: :scram,
        username: "user",
        password: "pass",
        mechanism_opts: %{algo: :sha256}
      }

      assert Scram.mechanism_name(config) == "SCRAM-SHA-256"
    end

    test "returns SCRAM-SHA-512 with sha512 algo" do
      config = %Config{
        mechanism: :scram,
        username: "user",
        password: "pass",
        mechanism_opts: %{algo: :sha512}
      }

      assert Scram.mechanism_name(config) == "SCRAM-SHA-512"
    end

    test "returns SCRAM-SHA-256 with empty mechanism_opts" do
      config = %Config{mechanism: :scram, username: "user", password: "pass", mechanism_opts: %{}}

      assert Scram.mechanism_name(config) == "SCRAM-SHA-256"
    end

    test "returns SCRAM-SHA-256 with nil mechanism_opts" do
      config = %Config{mechanism: :scram, username: "user", password: "pass", mechanism_opts: nil}

      assert Scram.mechanism_name(config) == "SCRAM-SHA-256"
    end
  end

  describe "authenticate/2" do
    # SCRAM authentication is complex and requires proper server responses.
    # These tests verify the delegation to ScramFlow happens correctly.

    test "delegates to ScramFlow with sha256 by default" do
      config = %Config{
        mechanism: :scram,
        username: "user",
        password: "pass",
        mechanism_opts: nil
      }

      # send_fun that fails immediately to verify delegation happens
      send_fun = fn _msg ->
        {:error, :test_stopped}
      end

      # Should attempt to call ScramFlow.authenticate which will call send_fun
      assert {:error, :test_stopped} = Scram.authenticate(config, send_fun)
    end

    test "delegates to ScramFlow with sha512 when specified" do
      config = %Config{
        mechanism: :scram,
        username: "user",
        password: "pass",
        mechanism_opts: %{algo: :sha512}
      }

      send_fun = fn _msg ->
        {:error, :test_stopped}
      end

      assert {:error, :test_stopped} = Scram.authenticate(config, send_fun)
    end

    test "passes username and password to ScramFlow" do
      config = %Config{
        mechanism: :scram,
        username: "testuser",
        password: "testpass",
        mechanism_opts: %{}
      }

      # The first message in SCRAM should contain the username
      send_fun = fn msg ->
        # client-first message contains "n=<username>"
        assert String.contains?(msg, "n=testuser")
        {:error, :test_validation_complete}
      end

      assert {:error, :test_validation_complete} = Scram.authenticate(config, send_fun)
    end
  end
end
