defmodule Kafka.SimpleConsumer.Test do
  use ExUnit.Case
  import Mock

  test "new returns a valid consumer" do
    response = TestHelper.generate_metadata_response(1,
      [%{:node_id => 0, :host => "localhost", :port => 9092},
       %{:node_id => 1, :host => "foo",       :port => 9092}],
      [%{:error_code => 0, :name => "test", partitions:
          [%{:error_code => 0, :id => 0, :leader => 0, :replicas => [0,1], :isrs => [0,1]},
           %{:error_code => 0, :id => 1, :leader => 0, :replicas => [0,1], :isrs => [0,1]}]},
       %{:error_code => 0, :name => "fnord", partitions:
          [%{:error_code => 0, :id => 0, :leader => 1, :replicas => [0,1], :isrs => [0,1]},
           %{:error_code => 0, :id => 1, :leader => 1, :replicas => [0,1], :isrs => [0,1]}]}])

    connection = %{:correlation_id => 1, :client_id => "client_id"}
    with_mock Kafka.Connection, [send: fn(_, _) -> {:ok, connection, response} end,
                                 connect: fn(_, _) -> {:ok, connection} end] do
      {:ok, consumer} = Kafka.SimpleConsumer.new([["localhost", 9092]], "foo", "test", 0)
      assert consumer.connection == connection
      assert consumer.broker == %{:host => "localhost", :port => 9092}
    end
  end

  test "new returns an error when the topic doesn't exist" do
    response = TestHelper.generate_metadata_response(1,
      [%{:node_id => 0, :host => "localhost", :port => 9092},
       %{:node_id => 1, :host => "foo",       :port => 9092}],
      [%{:error_code => 0, :name => "test", partitions:
          [%{:error_code => 0, :id => 0, :leader => 0, :replicas => [0,1], :isrs => [0,1]},
           %{:error_code => 0, :id => 1, :leader => 0, :replicas => [0,1], :isrs => [0,1]}]},
       %{:error_code => 0, :name => "fnord", partitions:
          [%{:error_code => 0, :id => 0, :leader => 1, :replicas => [0,1], :isrs => [0,1]},
           %{:error_code => 0, :id => 1, :leader => 1, :replicas => [0,1], :isrs => [0,1]}]}])

    connection = %{:correlation_id => 1, :client_id => "client_id"}
    with_mock Kafka.Connection, [send: fn(_, _) -> {:ok, connection, response} end,
                                 connect: fn(_, _) -> {:ok, connection} end] do
      assert {:error, _, _} = Kafka.SimpleConsumer.new([["localhost", 9092]], "foo", "non-existent", 0)
    end
  end

  test "new returns an error when the broker can't be found" do
    with_mock Kafka.Connection, [connect: fn(_, _) -> {:error, "some error"} end] do
      assert {:error, _} = Kafka.SimpleConsumer.new([["localhost", 9092]], "foo", "non-existent", 0)
    end
  end
end
