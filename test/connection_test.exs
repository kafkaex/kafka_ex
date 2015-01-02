defmodule ConnectionTest do
  use ExUnit.Case

  test "Connection.connect with list" do
    # Note: I'm merely asserting here that nothing is raised
    Kafka.Connection.connect([['localhost', 9092]])
  end

  test "Connection.connect with string" do
    Kafka.Connection.connect([["localhost", 9092]])
  end
end
