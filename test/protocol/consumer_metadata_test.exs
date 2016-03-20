defmodule KafkaEx.Protocol.ConsumerMetadata.Test do
  use ExUnit.Case, async: true

  test "create_request creates a valid consumer metadata request" do
    good_request = <<10 :: 16, 0 :: 16, 1 :: 32, 3 :: 16, "foo", 2 :: 16, "we" >>
    request = KafkaEx.Protocol.ConsumerMetadata.create_request(1, "foo", "we")
    assert request == good_request
  end

  test "parse_response correctly parses a valid response" do
    response = <<0, 0, 156, 65, 0, 0, 0, 0, 192, 6, 0, 14, 49, 57, 50, 46, 49, 54, 56, 46, 53, 57, 46, 49, 48, 51, 0, 0, 192, 6>>

    assert KafkaEx.Protocol.ConsumerMetadata.parse_response(response) == %KafkaEx.Protocol.ConsumerMetadata.Response{coordinator_id: 49158, coordinator_host: "192.168.59.103", coordinator_port: 49158, error_code: :no_error}
  end

  test "Response.broker_for_consumer_group returns correct coordinator_broker" do
    fake_socket = Port.open({:spawn, "cat"}, [])
    consumer_group_metadata = %KafkaEx.Protocol.ConsumerMetadata.Response{coordinator_host: "192.168.59.103", coordinator_id: 49162, coordinator_port: 49162, error_code: :no_error}

    brokers = [
        %KafkaEx.Protocol.Metadata.Broker{host: "192.168.0.1", port: 9092, socket: fake_socket},
        %KafkaEx.Protocol.Metadata.Broker{host: "192.168.59.103", port: 49162, socket: fake_socket}
    ]

    assert KafkaEx.Protocol.ConsumerMetadata.Response.broker_for_consumer_group(brokers, consumer_group_metadata) == %KafkaEx.Protocol.Metadata.Broker{host: "192.168.59.103", port: 49162, socket: fake_socket}
    Port.close(fake_socket)
  end

  test "Response.broker_for_consumer_group returns 'nil' when the broker's socket is closed" do
    fake_socket = Port.open({:spawn, "cat"}, [])
    Port.close(fake_socket)
    consumer_group_metadata = %KafkaEx.Protocol.ConsumerMetadata.Response{coordinator_host: "192.168.0.103", coordinator_id: 9092, coordinator_port: 9092, error_code: :no_error}

    brokers = [
        %KafkaEx.Protocol.Metadata.Broker{host: "192.168.0.1", port: 9092, socket: fake_socket},
        %KafkaEx.Protocol.Metadata.Broker{host: "192.168.0.103", port: 9092, socket: fake_socket}
    ]

    assert KafkaEx.Protocol.ConsumerMetadata.Response.broker_for_consumer_group(brokers, consumer_group_metadata) == nil
  end

  test "Response.broker_for_consumer_group returns 'nil' when the broker does not exist" do
    fake_socket = Port.open({:spawn, "cat"}, [])
    consumer_group_metadata = %KafkaEx.Protocol.ConsumerMetadata.Response{coordinator_host: "192.168.59.103", coordinator_id: 49162, coordinator_port: 49162, error_code: :no_error}

    brokers = [
        %KafkaEx.Protocol.Metadata.Broker{host: "192.168.0.1", port: 9092, socket: fake_socket},
        %KafkaEx.Protocol.Metadata.Broker{host: "192.168.0.103", port: 9092, socket: fake_socket}
    ]

    assert KafkaEx.Protocol.ConsumerMetadata.Response.broker_for_consumer_group(brokers, consumer_group_metadata) == nil
    Port.close(fake_socket)
  end
end
