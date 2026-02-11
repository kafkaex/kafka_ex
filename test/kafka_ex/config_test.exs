defmodule KafkaEx.ConfigTest do
  alias KafkaEx.Config

  use ExUnit.Case

  setup do
    # reset application env after each test
    env_before = Application.get_all_env(:kafka_ex)

    on_exit(fn ->
      # this is basically Application.put_all_env
      for {k, v} <- env_before do
        Application.put_env(:kafka_ex, k, v)
      end

      :ok
    end)

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

    Application.put_env(:kafka_ex, :ssl_options, foo: :bar)
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

  describe "OTP 26+ SSL compatibility" do
    setup do
      use_ssl = Application.get_env(:kafka_ex, :use_ssl)
      ssl_options = Application.get_env(:kafka_ex, :ssl_options)

      on_exit(fn ->
        Application.put_env(:kafka_ex, :use_ssl, use_ssl)
        Application.put_env(:kafka_ex, :ssl_options, ssl_options)
      end)
    end

    test "ssl_options raises helpful error when empty on OTP 26+" do
      otp_version = :erlang.system_info(:otp_release) |> List.to_integer()

      if otp_version >= 26 do
        Application.put_env(:kafka_ex, :use_ssl, true)
        Application.put_env(:kafka_ex, :ssl_options, [])

        error =
          assert_raise ArgumentError, fn ->
            Config.ssl_options()
          end

        # Verify error message contains helpful guidance
        assert error.message =~ "SSL is enabled but ssl_options is empty"
        assert error.message =~ "OTP 26+"
        assert error.message =~ "verify: :verify_none"
        assert error.message =~ "verify: :verify_peer"
        assert error.message =~ "cacerts: :public_key.cacerts_get()"
        assert error.message =~ "cacertfile:"
      else
        IO.inspect("Skipping test as OTP version is #{otp_version}")
      end
    end

    test "ssl_options works with verify_none on OTP 26+" do
      Application.put_env(:kafka_ex, :use_ssl, true)
      Application.put_env(:kafka_ex, :ssl_options, verify: :verify_none)

      assert [verify: :verify_none] == Config.ssl_options()
    end

    test "ssl_options works with verify_peer and cacerts on OTP 26+" do
      otp_version = :erlang.system_info(:otp_release) |> List.to_integer()

      if otp_version >= 26 do
        Application.put_env(:kafka_ex, :use_ssl, true)
        cacerts = :public_key.cacerts_get()

        Application.put_env(:kafka_ex, :ssl_options, verify: :verify_peer, cacerts: cacerts)

        assert [verify: :verify_peer, cacerts: cacerts] == Config.ssl_options()
      end
    end

    test "ssl_options works with verify_peer and cacertfile on OTP 26+" do
      Application.put_env(:kafka_ex, :use_ssl, true)
      Application.put_env(:kafka_ex, :ssl_options, verify: :verify_peer, cacertfile: "/path/to/ca-cert.pem")

      assert [verify: :verify_peer, cacertfile: "/path/to/ca-cert.pem"] == Config.ssl_options()
    end
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
    brokers = fn ->
      get_brokers(port: 8888)
    end

    Application.put_env(:kafka_ex, :brokers, brokers)

    assert get_brokers(port: 8888) == Config.brokers()
  end

  describe "disable_default_worker/0" do
    test "returns false by default" do
      Application.delete_env(:kafka_ex, :disable_default_worker)
      refute Config.disable_default_worker()
    end

    test "returns true when configured" do
      Application.put_env(:kafka_ex, :disable_default_worker, true)
      assert Config.disable_default_worker()
    end
  end

  describe "client_id/0" do
    test "returns 'kafka_ex' by default" do
      Application.delete_env(:kafka_ex, :client_id)
      assert "kafka_ex" == Config.client_id()
    end

    test "returns custom client_id when configured" do
      Application.put_env(:kafka_ex, :client_id, "my_custom_client")
      assert "my_custom_client" == Config.client_id()
    end
  end

  describe "default_consumer_group/0" do
    test "returns 'kafka_ex' by default" do
      Application.delete_env(:kafka_ex, :default_consumer_group)
      Application.delete_env(:kafka_ex, :consumer_group)
      assert "kafka_ex" == Config.default_consumer_group()
    end

    test "returns custom value when :default_consumer_group is set" do
      Application.put_env(:kafka_ex, :default_consumer_group, "my_group")
      assert "my_group" == Config.default_consumer_group()
    end

    test "falls back to :consumer_group key when :default_consumer_group is nil" do
      Application.put_env(:kafka_ex, :default_consumer_group, nil)
      Application.put_env(:kafka_ex, :consumer_group, "legacy_group")
      assert "legacy_group" == Config.default_consumer_group()
    end
  end

  describe "consumer_group/0 (deprecated)" do
    test "delegates to default_consumer_group/0" do
      Application.put_env(:kafka_ex, :default_consumer_group, "delegated_group")
      assert Config.consumer_group() == Config.default_consumer_group()
    end
  end

  describe "partitioner/0" do
    test "returns default partitioner when not configured" do
      Application.delete_env(:kafka_ex, :partitioner)
      assert KafkaEx.Producer.Partitioner.Default == Config.partitioner()
    end

    test "returns custom partitioner when configured" do
      Application.put_env(:kafka_ex, :partitioner, MyApp.CustomPartitioner)
      assert MyApp.CustomPartitioner == Config.partitioner()
    end
  end

  describe "use_ssl/0" do
    test "returns false by default" do
      Application.delete_env(:kafka_ex, :use_ssl)
      refute Config.use_ssl()
    end

    test "returns true when configured" do
      Application.put_env(:kafka_ex, :use_ssl, true)
      assert Config.use_ssl()
    end
  end

  describe "default_worker/0" do
    test "always returns :kafka_ex" do
      assert :kafka_ex == Config.default_worker()
    end
  end

  describe "server_impl/0" do
    test "always returns KafkaEx.Client" do
      assert KafkaEx.Client == Config.server_impl()
    end
  end

  describe "brokers/0" do
    test "returns nil when not configured" do
      Application.delete_env(:kafka_ex, :brokers)
      assert nil == Config.brokers()
    end

    test "raises when CSV broker has no port" do
      Application.put_env(:kafka_ex, :brokers, "example.com")

      assert_raise RuntimeError, ~r/Port not set/, fn ->
        Config.brokers()
      end
    end
  end

  describe "ssl_options/0 edge cases" do
    test "ssl_options returns empty list when ssl true and empty options on OTP < 26" do
      otp_version = :erlang.system_info(:otp_release) |> List.to_integer()

      if otp_version < 26 do
        Application.put_env(:kafka_ex, :use_ssl, true)
        Application.put_env(:kafka_ex, :ssl_options, [])
        assert [] == Config.ssl_options()
      end
    end

    test "ssl_options logs warning when use_ssl false but options are set" do
      Application.put_env(:kafka_ex, :use_ssl, false)
      Application.put_env(:kafka_ex, :ssl_options, cacertfile: "/path")

      # Should return empty list (ignoring the configured options)
      assert [] == Config.ssl_options()
    end
  end

  def get_brokers(opts) do
    port = Keyword.get(opts, :port)

    [
      {"elixir-lang.org", port}
    ]
  end
end
