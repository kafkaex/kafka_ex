defmodule KafkaEx.ConfigTest do
  alias KafkaEx.Config

  use ExUnit.Case

  setup do
    # reset application env after each test
    env_before = Application.get_all_env(:kafka_ex)
    on_exit fn ->
      # this is basically Application.put_all_env
      for {k, v} <- env_before do
        Application.put_env(:kafka_ex, k, v)
      end
      :ok
    end
    :ok
  end

  test "ssl_options returns the correct value when configured properly" do
    Application.put_env(:kafka_ex, :use_ssl, true)
    ssl_options = Application.get_env(:kafka_ex, :ssl_options)
    assert ssl_options == Config.ssl_options()
  end

  test "ssl_options returns an empty list when use_ssl is false" do
    Application.put_env(:kafka_ex, :use_ssl, false)
    Application.put_env(:kafka_ex, :ssl_options, nil)
    assert [] == Config.ssl_options()

    Application.put_env(:kafka_ex, :ssl_options, [foo: :bar])
    assert [] == Config.ssl_options()
  end

  test "ssl_options raises an error if use_ssl is true and ssl_options " <>
    "are invalid" do
    Application.put_env(:kafka_ex, :use_ssl, true)

    # when ssl_options is not set we should get an error
    Application.put_env(:kafka_ex, :ssl_options, nil)
    assert_raise(ArgumentError, ~r/invalid ssl_options/, &Config.ssl_options/0)

    # should also get an error if ssl_options is not a list
    Application.put_env(:kafka_ex, :ssl_options, %{cacertfile: "/ssl/ca-cert"})
    assert_raise(ArgumentError, ~r/invalid ssl_options/, &Config.ssl_options/0)
  end

  test "brokers with list of hosts" do
    brokers = [{"example.com", 9092}]
    Application.put_env(:kafka_ex, :brokers, brokers)

    assert brokers == Config.brokers()
  end

  test "brokers with a csv of hosts" do
    brokers = " example.com:3452,one.example.com:4534, two.example.com:9999 "

    parsed_brokers = [
      {"example.com", 3452},
      {"one.example.com", 4534},
      {"two.example.com", 9999}
    ]

    Application.put_env(:kafka_ex, :brokers, brokers)
    assert parsed_brokers == Config.brokers()
  end

  test "brokers with lazy configuration using mod, fun, and args" do
    opts = [port: 8888]
    brokers = {KafkaEx.ConfigTest, :get_brokers, [opts]}
    Application.put_env(:kafka_ex, :brokers, brokers)

    assert get_brokers(port: 8888) == Config.brokers()
  end

  test "brokers with lazy configuration using fun" do
    brokers =
      fn ->
        get_brokers(port: 8888)
      end
    Application.put_env(:kafka_ex, :brokers, brokers)

    assert get_brokers(port: 8888) == Config.brokers()
  end

  def get_brokers(opts) do
    port = Keyword.get(opts, :port)
    [
      {"elixir-lang.org", port}
    ]
  end
end
