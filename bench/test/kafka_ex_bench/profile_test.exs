defmodule KafkaExBench.ProfileTest do
  use ExUnit.Case, async: true
  alias KafkaExBench.Profile

  test "direct ssl goes straight to the brokers" do
    opts = Profile.client_opts(:direct, :ssl)
    assert opts[:brokers] == [{"localhost", 9092}, {"localhost", 9093}, {"localhost", 9094}]
    assert opts[:use_ssl] == true
    assert opts[:consumer_group] == :no_consumer_group
    assert opts[:allow_auto_topic_creation] == false
    refute Keyword.has_key?(opts, :auth)
  end

  test "latency scram goes through the proxy ports with scram auth" do
    opts = Profile.client_opts(:latency, :scram)
    assert opts[:brokers] == [{"localhost", 9592}, {"localhost", 9593}, {"localhost", 9594}]
    assert %KafkaEx.Auth.Config{mechanism: :scram} = opts[:auth]
  end

  test "parse_security! accepts ssl and scram only" do
    assert Profile.parse_security!("ssl") == :ssl
    assert Profile.parse_security!("scram") == :scram
    assert_raise FunctionClauseError, fn -> Profile.parse_security!("plain") end
  end

  test "parse! rejects unknown profiles" do
    assert Profile.parse!("latency") == :latency
    assert_raise FunctionClauseError, fn -> Profile.parse!("wan") end
  end
end
