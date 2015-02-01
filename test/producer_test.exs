defmodule Kafka.Producer.Test do
  use ExUnit.Case
  import Mock

  test "new returns a valid producer" do
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
    with_mock Kafka.Connection, [send_and_return_response: fn(_, _) -> {:ok, connection, response} end,
                                 connect: fn(_, _) -> {:ok, connection} end] do
      {:ok, producer} = Kafka.Producer.new([["localhost", 9092]], "foo", "test", 0)
      assert producer.connection == connection
      assert producer.broker == %{:host => "localhost", :port => 9092}
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
    with_mock Kafka.Connection, [send_and_return_response: fn(_, _) -> {:ok, connection, response} end,
                                 connect: fn(_, _) -> {:ok, connection} end] do
      assert {:error, _, _} = Kafka.Producer.new([["localhost", 9092]], "foo", "non-existent", 0)
    end
  end

  test "new returns an error when the broker can't be found" do
    with_mock Kafka.Connection, [connect: fn(_, _) -> {:error, "some error"} end] do
      assert {:error, _} = Kafka.Producer.new([["localhost", 9092]], "foo", "non-existent", 0)
    end
  end

  test "produce with required_acks == 0 connects to the broker and returns nothing" do
    connection = %{:correlation_id => 1, :client_id => "client_id"}
    topic = "test"
    message = "message"
    producer = %{:connection => connection, :broker => %{:host => "foo", :port => 9092}, :metadata => %{:timestamp => Kafka.Helper.get_timestamp},
      :topic => topic, :partition => 0}
    binary_message = Kafka.Protocol.Produce.create_request(connection, topic, 0, message)
    with_mock Kafka.Connection, [send: fn(_, connection) -> {:ok, connection} end,
                                 connect: fn(_, _) -> {:ok, connection} end] do
      assert {:ok, producer} == Kafka.Producer.produce(producer, message)
      assert called Kafka.Connection.send(binary_message, %{:correlation_id => 1, :client_id => "client_id"})
    end
  end

  test "produce with required_acks == 1 connects to the broker and returns a response" do
    connection = %{:correlation_id => 1, :client_id => "client_id"}
    topic = "test"
    message = "message"
    producer = %{:connection => connection, :broker => %{:host => "foo", :port => 9092}, :metadata => %{:timestamp => Kafka.Helper.get_timestamp},
      :topic => topic, :partition => 0}
    binary_message = Kafka.Protocol.Produce.create_request(connection, topic, 0, message, nil, 1)
    response = << 0 :: 32, 1 :: 32, 4 :: 16, "test" :: binary, 1 :: 32, 0 :: 32, 0 :: 16, 10 :: 64 >>
    with_mock Kafka.Connection, [send: fn(_, connection) -> {:ok, connection} end,
                                 send_and_return_response: fn(_, connection) -> {:ok, connection, response} end,
                                 connect: fn(_, _) -> {:ok, connection} end] do
      assert {:ok, %{"test" => %{0 => %{error_code: 0, offset: 10}}}, producer} == Kafka.Producer.produce(producer, message, nil, 1)
      assert called Kafka.Connection.send_and_return_response(binary_message, %{:correlation_id => 1, :client_id => "client_id"})
    end
  end

  test "produce returns error when it can't send to broker" do
    connection = %{:correlation_id => 1, :client_id => "client_id"}
    topic = "test"
    message = "message"
    producer = %{:connection => connection, :broker => %{:host => "foo", :port => 9092}, :metadata => %{:timestamp => Kafka.Helper.get_timestamp},
      :topic => topic, :partition => 0}
    with_mock Kafka.Connection, [send: fn(_, _) -> {:error, :boom} end,
                                 connect: fn(_, _) -> {:ok, connection} end] do
      assert {:error, :boom, producer} == Kafka.Producer.produce(producer, message)
    end
  end

  test "produce updates metadata after 5 minutes and doesn't rebalance if not necessary" do
    topic = "test"
    binary_response = << 0 :: 32, 1 :: 32, 4 :: 16, "test" :: binary, 1 :: 32, 0 :: 32, 0 :: 16, 10 :: 64 >>
    connection = %{:correlation_id => 1, :client_id => "client_id"}
    producer = %{:connection => connection, :broker => %{:host => "foo", :port => 9092}, :metadata => %{:timestamp => Kafka.Helper.get_timestamp},
      :topic => topic, :partition => 0}
    message = "message"
    with_mock Kafka.Connection, [send: fn(_, connection) -> {:ok, connection} end,
                                 connect: fn(_, _) -> {:ok, connection} end,
                                 send_and_return_response: fn(_, _) -> {:ok, connection, binary_response} end,
                                 close: fn(_) -> :ok end] do
      with_mock Kafka.Metadata, [update: fn(_) -> {:ok, :updated, %{}} end,
                                 get_broker: fn(_, _, _) -> {:ok, %{:host => "localhost", :port => 9092}, %{}} end] do
        Kafka.Producer.produce(producer, message)
        assert !called Kafka.Connection.connect(%{host: "foo", port: 9092}, "client_id")
      end
    end
  end

  test "produce updates metadata after 5 minutes and rebalances if necessary" do
    topic = "test"
    binary_response = << 0 :: 32, 1 :: 32, 4 :: 16, "test" :: binary, 1 :: 32, 0 :: 32, 0 :: 16, 10 :: 64 >>
    connection = %{:correlation_id => 1, :client_id => "client_id"}
    producer = %{:connection => connection, :broker => %{:host => "localhost", :port => 9092}, :metadata => %{:timestamp => Kafka.Helper.get_timestamp - 5 * 60 * 1000},
      :topic => topic, :partition => 0}
    message = "message"
    with_mock Kafka.Connection, [send: fn(_, connection) -> {:ok, connection} end,
                                 connect: fn(_, _) -> {:ok, connection} end,
                                 send_and_return_response: fn(_, _) -> {:ok, connection, binary_response} end,
                                 close: fn(_) -> :ok end] do
      with_mock Kafka.Metadata, [update: fn(_) -> {:ok, :updated, %{}} end,
                                 get_broker: fn(_, _, _) -> {:ok, %{:host => "foo", :port => 9092}, %{}} end] do
        Kafka.Producer.produce(producer, message)
        assert called Kafka.Connection.connect(%{host: "foo", port: 9092}, "client_id")
      end
    end
  end
end
