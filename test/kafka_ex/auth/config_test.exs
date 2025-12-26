defmodule KafkaEx.Auth.ConfigTest do
  use ExUnit.Case, async: true
  alias KafkaEx.Auth.Config

  test "new/1 builds struct and redacts password in inspect" do
    cfg =
      Config.new(%{
        mechanism: :scram,
        username: "alice",
        password: "secret",
        mechanism_opts: %{algo: :sha512}
      })

    assert %Config{mechanism: :scram, username: "alice", password: "secret"} = cfg

    inspected = inspect(cfg)
    refute inspected =~ "secret"
    assert inspected =~ "***REDACTED***"
  end

  test "from_env/0 builds config if use_sasl is true" do
    Application.put_env(:kafka_ex, :use_sasl, true)
    Application.put_env(:kafka_ex, :sasl, %{mechanism: :plain, username: "bob", password: "pw"})

    cfg = Config.from_env()
    assert %Config{mechanism: :plain, username: "bob", password: "pw"} = cfg
  after
    Application.delete_env(:kafka_ex, :use_sasl)
    Application.delete_env(:kafka_ex, :sasl)
  end

  test "raises on missing required keys" do
    assert_raise ArgumentError, fn ->
      Config.new(%{mechanism: :plain})
    end
  end

  describe "Inspect redaction" do
    test "redacts AWS credentials in mechanism_opts" do
      cfg =
        Config.new(%{
          mechanism: :msk_iam,
          mechanism_opts: %{
            region: "us-east-1",
            access_key_id: "AKIATEST123",
            secret_access_key: "verysecretkey",
            session_token: "sessiontoken123"
          }
        })

      inspected = inspect(cfg)

      refute inspected =~ "AKIATEST123"
      refute inspected =~ "verysecretkey"
      refute inspected =~ "sessiontoken123"
      assert inspected =~ "***REDACTED***"
      assert inspected =~ "us-east-1"
    end

    test "keeps non-sensitive fields visible" do
      cfg =
        Config.new(%{
          mechanism: :msk_iam,
          mechanism_opts: %{
            region: "eu-west-1",
            broker_host: "b-1.test.kafka.amazonaws.com",
            broker_port: 9098,
            secret_access_key: "mysecretvalue"
          }
        })

      inspected = inspect(cfg)

      assert inspected =~ "eu-west-1"
      assert inspected =~ "b-1.test.kafka.amazonaws.com"
      assert inspected =~ "9098"
      refute inspected =~ "mysecretvalue"
    end
  end
end
