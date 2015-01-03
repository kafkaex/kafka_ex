defmodule MetadataTest do
  use ExUnit.Case, async: true
  import Mock

  test "get_metadata for single node and topic" do
    response = TestHelper.generate_metadata_response(1,
      [%{node_id: 0, host: "localhost", port: 9092}],
      [%{error_code: 0, name: "test", partitions:
          [%{error_code: 0, id: 0, leader: 0, replicas: [0], isrs: [0]}]}])

    with_mock Kafka.Connection, [send: fn(_, _) -> response end] do
      {broker_map, topic_map} = Kafka.Metadata.get_metadata(nil, 1, "foo")
      assert broker_map == %{0 => %{host: "localhost", port: 9092, socket: nil}}
      assert topic_map  == %{"test" =>
        [error_code: 0, partitions: %{0 => %{error_code: 0, isrs: [0], leader: 0, replicas: [0]}}]}
    end
  end

  test "get_metadata for multiple nodes and topics" do
    response = TestHelper.generate_metadata_response(1,
      [%{node_id: 0, host: "localhost", port: 9092},
       %{node_id: 1, host: "foo",       port: 9092}],
      [%{error_code: 0, name: "test", partitions:
          [%{error_code: 0, id: 0, leader: 0, replicas: [0,1], isrs: [0,1]},
           %{error_code: 0, id: 1, leader: 0, replicas: [0,1], isrs: [0,1]}]},
       %{error_code: 0, name: "fnord", partitions:
          [%{error_code: 0, id: 0, leader: 1, replicas: [0,1], isrs: [0,1]},
           %{error_code: 0, id: 1, leader: 1, replicas: [0,1], isrs: [0,1]}]}])

    with_mock Kafka.Connection, [send: fn(_, _) -> response end] do
      {broker_map, topic_map} = Kafka.Metadata.get_metadata(nil, 1, "foo")
      assert broker_map == %{0 =>
        %{host: "localhost", port: 9092, socket: nil}, 1 => %{host: "foo", port: 9092, socket: nil}}
      assert topic_map  == %{"fnord" => [error_code: 0,
          partitions: %{0 => %{error_code: 0, isrs: [0, 1], leader: 1, replicas: [0, 1]},
            1 => %{error_code: 0, isrs: [0, 1], leader: 1, replicas: [0, 1]}}],
        "test" => [error_code: 0,
          partitions: %{0 => %{error_code: 0, isrs: [0, 1], leader: 0, replicas: [0, 1]},
            1 => %{error_code: 0, isrs: [0, 1], leader: 0, replicas: [0, 1]}}]}
    end
  end
end
