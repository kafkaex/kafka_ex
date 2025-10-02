# test/kafka_ex/auth/version_support_test.exs
defmodule KafkaEx.Auth.VersionSupportTest do
  use ExUnit.Case
  alias KafkaEx.Auth.SASL.VersionSupport
  alias KafkaEx.Auth.Config
  alias KafkaEx.Socket

  describe "sasl_support_level/0" do
    test "detects unsupported versions" do
      for version <- ["0.8.0", "0.8.2", "0.8.5"] do
        Application.put_env(:kafka_ex, :kafka_version, version)
        assert VersionSupport.sasl_support_level() == :unsupported
      end
    end

    test "detects plain-only support" do
      for version <- ["0.9.0", "0.9.1", "0.9.3"] do
        Application.put_env(:kafka_ex, :kafka_version, version)
        assert VersionSupport.sasl_support_level() == :plain_only
      end
    end

    test "detects plain with API versions" do
      for version <- ["0.10.0", "0.10.1"] do
        Application.put_env(:kafka_ex, :kafka_version, version)
        assert VersionSupport.sasl_support_level() == :plain_with_api_versions
      end
    end

    test "detects full support" do
      for version <- ["0.10.2", "0.10.3", "0.11.0", "1.0.0", "2.8.0", "kayrock", :default] do
        Application.put_env(:kafka_ex, :kafka_version, version)
        assert VersionSupport.sasl_support_level() == :full
      end
    end
  end

  describe "validate_config/2" do
    setup do
      ssl_socket = %Socket{ssl: true, socket: :dummy}
      plain_socket = %Socket{ssl: false, socket: :dummy}
      {:ok, ssl_socket: ssl_socket, plain_socket: plain_socket}
    end

    test "rejects SASL on old Kafka", %{ssl_socket: socket} do
      Application.put_env(:kafka_ex, :kafka_version, "0.8.0")

      config = %Config{
        mechanism: :plain,
        username: "test",
        password: "secret",
        mechanism_opts: %{}
      }

      assert {:error, :sasl_requires_kafka_0_9_plus} =
               VersionSupport.validate_config(config, socket)
    end

    test "rejects SCRAM on Kafka 0.9", %{ssl_socket: socket} do
      Application.put_env(:kafka_ex, :kafka_version, "0.9.0")

      config = %Config{
        mechanism: :scram_sha_256,
        username: "test",
        password: "secret",
        mechanism_opts: %{}
      }

      assert {:error, :scram_requires_kafka_0_10_2_plus} =
               VersionSupport.validate_config(config, socket)
    end

    test "rejects PLAIN without TLS", %{plain_socket: socket} do
      Application.put_env(:kafka_ex, :kafka_version, "1.0.0")

      config = %Config{
        mechanism: :plain,
        username: "test",
        password: "secret",
        mechanism_opts: %{}
      }

      assert {:error, :plain_requires_tls} =
               VersionSupport.validate_config(config, socket)
    end

    test "accepts PLAIN with TLS on 0.9", %{ssl_socket: socket} do
      Application.put_env(:kafka_ex, :kafka_version, "0.9.0")

      config = %Config{
        mechanism: :plain,
        username: "test",
        password: "secret",
        mechanism_opts: %{}
      }

      assert :ok = VersionSupport.validate_config(config, socket)
    end

    test "accepts SCRAM on 0.10.2+", %{ssl_socket: socket} do
      Application.put_env(:kafka_ex, :kafka_version, "0.10.2")

      config = %Config{
        mechanism: :scram_sha_512,
        username: "test",
        password: "secret",
        mechanism_opts: %{}
      }

      assert :ok = VersionSupport.validate_config(config, socket)
    end
  end

  describe "check_api_versions?/0" do
    test "returns false for Kafka 0.9" do
      Application.put_env(:kafka_ex, :kafka_version, "0.9.0")
      refute VersionSupport.check_api_versions?()
    end

    test "returns true for Kafka 0.10+" do
      for version <- ["0.10.0", "0.10.2", "1.0.0", "kayrock"] do
        Application.put_env(:kafka_ex, :kafka_version, version)
        assert VersionSupport.check_api_versions?()
      end
    end
  end
end
