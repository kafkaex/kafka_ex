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

  test "ssl_options raises an error if cacertfile is invalid" do
    Application.put_env(:kafka_ex, :use_ssl, true)
    ssl_options = Application.get_env(:kafka_ex, :ssl_options)

    key = :cacertfile

    # the option may be omitted - it is up to :ssl to determine if this is ok
    without_key = Keyword.delete(ssl_options, key)
    Application.put_env(:kafka_ex, :ssl_options, without_key)
    assert without_key == Config.ssl_options()

    with_invalid_file = Keyword.put(ssl_options, key, "./should_not_exist")
    Application.put_env(:kafka_ex, :ssl_options, with_invalid_file)
    assert_raise(ArgumentError, ~r/could not/, &Config.ssl_options/0)
  end

  test "ssl_options raises an error if certfile is invalid" do
    Application.put_env(:kafka_ex, :use_ssl, true)
    ssl_options = Application.get_env(:kafka_ex, :ssl_options)

    key = :certfile

    # the option may be omitted - it is up to :ssl to determine if this is ok
    without_key = Keyword.delete(ssl_options, key)
    Application.put_env(:kafka_ex, :ssl_options, without_key)
    assert without_key == Config.ssl_options()

    with_invalid_file = Keyword.put(ssl_options, key, "./should_not_exist")
    Application.put_env(:kafka_ex, :ssl_options, with_invalid_file)
    assert_raise(ArgumentError, ~r/could not/, &Config.ssl_options/0)
  end

  test "ssl_options raises an error if keyfile is invalid" do
    Application.put_env(:kafka_ex, :use_ssl, true)
    ssl_options = Application.get_env(:kafka_ex, :ssl_options)

    key = :keyfile

    # the option may be omitted - it is up to :ssl to determine if this is ok
    without_key = Keyword.delete(ssl_options, key)
    Application.put_env(:kafka_ex, :ssl_options, without_key)
    assert without_key == Config.ssl_options()

    with_invalid_file = Keyword.put(ssl_options, key, "./should_not_exist")
    Application.put_env(:kafka_ex, :ssl_options, with_invalid_file)
    assert_raise(ArgumentError, ~r/could not/, &Config.ssl_options/0)
  end
end
