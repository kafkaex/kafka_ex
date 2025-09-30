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
end
