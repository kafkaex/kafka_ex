# test/kafka_ex/auth/version_support_test.exs
defmodule KafkaEx.Auth.VersionSupportTest do
  use ExUnit.Case
  alias KafkaEx.Auth.SASL.VersionSupport
  alias KafkaEx.Auth.Config
  alias KafkaEx.Socket

  describe "sasl_support_level/0" do
    test "returns full support" do
      assert VersionSupport.sasl_support_level() == :full
    end
  end

  describe "validate_config/2" do
    setup do
      ssl_socket = %Socket{ssl: true, socket: :dummy}
      plain_socket = %Socket{ssl: false, socket: :dummy}
      {:ok, ssl_socket: ssl_socket, plain_socket: plain_socket}
    end

    test "rejects PLAIN without TLS", %{plain_socket: socket} do
      config = %Config{
        mechanism: :plain,
        username: "test",
        password: "secret",
        mechanism_opts: %{}
      }

      assert {:error, :plain_requires_tls} =
               VersionSupport.validate_config(config, socket)
    end

    test "accepts PLAIN with TLS", %{ssl_socket: socket} do
      config = %Config{
        mechanism: :plain,
        username: "test",
        password: "secret",
        mechanism_opts: %{}
      }

      assert :ok = VersionSupport.validate_config(config, socket)
    end

    test "accepts SCRAM with TLS", %{ssl_socket: socket} do
      config = %Config{
        mechanism: :scram_sha_512,
        username: "test",
        password: "secret",
        mechanism_opts: %{}
      }

      assert :ok = VersionSupport.validate_config(config, socket)
    end

    test "accepts SCRAM without TLS", %{plain_socket: socket} do
      # SCRAM is secure even without TLS (password not sent in clear)
      config = %Config{
        mechanism: :scram_sha_256,
        username: "test",
        password: "secret",
        mechanism_opts: %{}
      }

      assert :ok = VersionSupport.validate_config(config, socket)
    end
  end

  describe "check_api_versions?/0" do
    test "returns true" do
      assert VersionSupport.check_api_versions?()
    end
  end
end
