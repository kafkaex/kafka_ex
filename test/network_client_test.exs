defmodule KafkaEx.NetworkClient.Test do
  use ExUnit.Case

  test "format_host returns Erlang IP address format if IP address string is specified" do
    assert {100, 20, 3, 4} == KafkaEx.NetworkClient.format_host("100.20.3.4")
  end

  test "format_host returns the char list version of the string passed in if host is not IP address" do
    assert 'host' == KafkaEx.NetworkClient.format_host("host")
  end

  test "format_host handles hosts with embedded digits correctly" do
    assert 'host0' == KafkaEx.NetworkClient.format_host("host0")
  end
end
