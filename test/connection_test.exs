defmodule ConnectionTest do
  use ExUnit.Case
  import Mock

  test "Connection.connect with list" do
    # Note: I'm merely asserting here that nothing is raised
    Kafka.Connection.connect([['localhost', 9092]])
  end

  test "Connection.connect with string" do
    Kafka.Connection.connect([["localhost", 9092]])
  end

  test "connection returns socket and correlation id" do
    with_mock :gen_tcp, [:unstick], [connect: fn(_, _, _) -> {:ok, %{}} end] do
      {:ok, connection} = Kafka.Connection.connect([["localhost", 9092]])
      assert connection == %{correlation_id: 1, socket: %{}}
    end
  end

  test "send increments the correlation id" do
    with_mock :gen_tcp, [:unstick], [connect: fn(_, _, _) -> {:ok, %{}} end,
                                     send:    fn(_, _)  -> send(self, {:tcp, nil, << >>}) end] do
      {:ok, connection} = Kafka.Connection.connect([["localhost", 9092]])
      {connection, data} = Kafka.Connection.send(connection, "foo")
      assert connection.correlation_id == 2
    end
  end
end
