defmodule KafkaEx.KayrockCompatibilityTest do
  @moduledoc """
  These are tests using the original KafkaEx API with the kayrock server

  These mostly come from the original integration_test.exs file
  """

  use ExUnit.Case

  @moduletag :new_client

  alias KafkaEx.New.Client
  alias KafkaEx.Protocol, as: Proto
  alias KafkaEx.Protocol.Metadata.Response, as: MetadataResponse
  alias KafkaEx.Protocol.Metadata.Broker
  alias KafkaEx.Protocol.Metadata.TopicMetadata
  alias KafkaEx.Protocol.Offset.Response, as: OffsetResponse

  alias KafkaEx.New.Structs.ClusterMetadata
  alias KafkaEx.New.KafkaExAPI

  setup do
    {:ok, pid} = KafkaEx.start_link_worker(:no_name, server_impl: KafkaEx.New.Client)

    {:ok, %{client: pid}}
  end

  test "Creates a worker even when the one of the provided brokers is not available" do
    uris = Application.get_env(:kafka_ex, :brokers) ++ [{"bad_host", 9000}]
    {:ok, args} = KafkaEx.build_worker_options(uris: uris)

    {:ok, pid} = Client.start_link(args, :no_name)

    assert Process.alive?(pid)
  end

  test "worker updates metadata after specified interval" do
    {:ok, args} = KafkaEx.build_worker_options(metadata_update_interval: 100)
    {:ok, pid} = Client.start_link(args, :no_name)
    previous_corr_id = KafkaExAPI.correlation_id(pid)

    :timer.sleep(105)
    curr_corr_id = KafkaExAPI.correlation_id(pid)
    refute curr_corr_id == previous_corr_id
  end

  test "get metadata", %{client: client} do
    metadata = KafkaEx.metadata(worker_name: client, topic: "test0p8p0")

    %MetadataResponse{topic_metadatas: topic_metadatas, brokers: brokers} = metadata

    assert is_list(topic_metadatas)
    [topic_metadata | _] = topic_metadatas
    assert %TopicMetadata{} = topic_metadata
    refute topic_metadatas == []
    assert is_list(brokers)
    [broker | _] = brokers
    assert %Broker{} = broker
  end

  test "list offsets", %{client: client} do
    topic = "test0p8p0"

    resp = KafkaEx.offset(topic, 0, :earliest, client)

    [%OffsetResponse{topic: ^topic, partition_offsets: [partition_offsets]}] = resp

    %{error_code: :no_error, offset: [offset], partition: 0} = partition_offsets
    assert offset >= 0
  end

  test "produce/4 without an acq required returns :ok", %{client: client} do
    assert KafkaEx.produce("food", 0, "hey",
             worker_name: client,
             required_acks: 0
           ) == :ok
  end

  test "produce/4 with ack required returns an ack", %{client: client} do
    {:ok, offset} =
      KafkaEx.produce(
        "food",
        0,
        "hey",
        worker_name: client,
        required_acks: 1
      )

    assert is_integer(offset)
    refute offset == nil
  end

  test "produce without an acq required returns :ok", %{client: client} do
    assert KafkaEx.produce(
             %Proto.Produce.Request{
               topic: "food",
               partition: 0,
               required_acks: 0,
               messages: [%Proto.Produce.Message{value: "hey"}]
             },
             worker_name: client
           ) == :ok
  end

  test "produce with ack required returns an ack", %{client: client} do
    {:ok, offset} =
      KafkaEx.produce(
        %Proto.Produce.Request{
          topic: "food",
          partition: 0,
          required_acks: 1,
          messages: [%Proto.Produce.Message{value: "hey"}]
        },
        worker_name: client
      )

    refute offset == nil
  end

  test "produce creates log for a non-existing topic", %{client: client} do
    random_string = TestHelper.generate_random_string()

    KafkaEx.produce(
      %Proto.Produce.Request{
        topic: random_string,
        partition: 0,
        required_acks: 1,
        messages: [%Proto.Produce.Message{value: "hey"}]
      },
      worker_name: client
    )

    {:ok, cluster_metadata} = KafkaExAPI.cluster_metadata(client)

    assert random_string in ClusterMetadata.known_topics(cluster_metadata)
  end

  test "fetch does not blow up with incomplete bytes", %{client: client} do
    KafkaEx.fetch("food", 0,
      offset: 0,
      max_bytes: 100,
      auto_commit: false,
      worker_name: client
    )
  end

  test "fetch returns ':topic_not_found' for non-existing topic", %{
    client: client
  } do
    random_string = TestHelper.generate_random_string()

    assert KafkaEx.fetch(random_string, 0, offset: 0, worker_name: client) ==
             :topic_not_found
  end

  test "fetch works", %{client: client} do
    topic_name = TestHelper.generate_random_string()

    {:ok, offset} =
      KafkaEx.produce(
        %Proto.Produce.Request{
          topic: topic_name,
          partition: 0,
          required_acks: 1,
          messages: [%Proto.Produce.Message{value: "hey foo"}]
        },
        worker_name: client
      )

    fetch_responses =
      KafkaEx.fetch(topic_name, 0,
        offset: 0,
        auto_commit: false,
        worker_name: client
      )

    [fetch_response | _] = fetch_responses

    message = fetch_response.partitions |> hd |> Map.get(:message_set) |> hd

    assert message.partition == 0
    assert message.topic == topic_name
    assert message.value == "hey foo"
    assert message.offset == offset
  end

  test "fetch with empty topic", %{client: client} do
    random_string = TestHelper.generate_random_string()

    response =
      KafkaEx.fetch(random_string, 0,
        offset: 0,
        auto_commit: false,
        worker_name: client
      )

    assert response == :topic_not_found
  end

  test "fetch nonexistent offset", %{client: client} do
    random_string = TestHelper.generate_random_string()

    {:ok, offset} =
      KafkaEx.produce(
        %Proto.Produce.Request{
          topic: random_string,
          partition: 0,
          required_acks: 1,
          messages: [%Proto.Produce.Message{value: "hey foo"}]
        },
        worker_name: client
      )

    fetch_responses =
      KafkaEx.fetch(random_string, 0,
        offset: offset + 5,
        auto_commit: false,
        worker_name: client
      )

    [fetch_response | _] = fetch_responses
    [partition | _] = fetch_response.partitions
    assert partition.error_code == :offset_out_of_range
  end

  test "fetch nonexistent partition", %{client: client} do
    random_string = TestHelper.generate_random_string()

    {:ok, offset} =
      KafkaEx.produce(
        %Proto.Produce.Request{
          topic: random_string,
          partition: 0,
          required_acks: 1,
          messages: [%Proto.Produce.Message{value: "hey foo"}]
        },
        worker_name: client
      )

    fetch_responses =
      KafkaEx.fetch(random_string, 99,
        offset: offset + 5,
        auto_commit: false,
        worker_name: client
      )

    assert fetch_responses == :topic_not_found
  end

  test "earliest_offset retrieves offset of 0", %{client: client} do
    random_string = TestHelper.generate_random_string()

    KafkaEx.produce(
      %Proto.Produce.Request{
        topic: random_string,
        partition: 0,
        required_acks: 1,
        messages: [%Proto.Produce.Message{value: "hey"}]
      },
      worker_name: client
    )

    offset_response = KafkaEx.earliest_offset(random_string, 0, client) |> hd

    offset = offset_response.partition_offsets |> hd |> Map.get(:offset) |> hd

    assert offset == 0
  end

  test "latest_offset retrieves offset of 0 for non-existing topic", %{
    client: client
  } do
    random_string = TestHelper.generate_random_string()

    {:ok, produce_offset} =
      KafkaEx.produce(
        %Proto.Produce.Request{
          topic: random_string,
          partition: 0,
          required_acks: 1,
          messages: [%Proto.Produce.Message{value: "hey"}]
        },
        worker_name: client
      )

    :timer.sleep(300)
    offset_response = KafkaEx.latest_offset(random_string, 0, client) |> hd
    offset = offset_response.partition_offsets |> hd |> Map.get(:offset) |> hd

    assert offset == produce_offset + 1
  end

  test "latest_offset retrieves a non-zero offset for a topic published to", %{
    client: client
  } do
    random_string = TestHelper.generate_random_string()

    KafkaEx.produce(
      %Proto.Produce.Request{
        topic: random_string,
        partition: 0,
        required_acks: 1,
        messages: [%Proto.Produce.Message{value: "foo"}]
      },
      worker_name: client
    )

    [offset_response] =
      TestHelper.wait_for_any(fn ->
        KafkaEx.latest_offset(random_string, 0, client)
      end)

    offset = offset_response.partition_offsets |> hd |> Map.get(:offset) |> hd

    assert offset != 0
  end

  # compression
  test "compresses / decompresses using gzip", %{client: client} do
    random_string = TestHelper.generate_random_string()

    message1 = %Proto.Produce.Message{value: "value 1"}
    message2 = %Proto.Produce.Message{key: "key 2", value: "value 2"}
    messages = [message1, message2]

    produce_request = %Proto.Produce.Request{
      topic: random_string,
      partition: 0,
      required_acks: 1,
      compression: :gzip,
      messages: messages
    }

    {:ok, offset} = KafkaEx.produce(produce_request, worker_name: client)

    fetch_response =
      KafkaEx.fetch(random_string, 0,
        offset: 0,
        auto_commit: false,
        worker_name: client
      )
      |> hd

    [got_message1, got_message2] = fetch_response.partitions |> hd |> Map.get(:message_set)

    assert got_message1.key == message1.key
    assert got_message1.value == message1.value
    assert got_message1.offset == offset
    assert got_message2.key == message2.key
    assert got_message2.value == message2.value
    assert got_message2.offset == offset + 1
  end

  test "compresses / decompresses using snappy", %{client: client} do
    random_string = TestHelper.generate_random_string()

    message1 = %Proto.Produce.Message{value: "value 1"}
    message2 = %Proto.Produce.Message{key: "key 2", value: "value 2"}
    messages = [message1, message2]

    produce_request = %Proto.Produce.Request{
      topic: random_string,
      partition: 0,
      required_acks: 1,
      compression: :snappy,
      messages: messages
    }

    {:ok, offset} = KafkaEx.produce(produce_request, worker_name: client)

    fetch_response =
      KafkaEx.fetch(random_string, 0,
        offset: 0,
        auto_commit: false,
        worker_name: client
      )
      |> hd

    [got_message1, got_message2] = fetch_response.partitions |> hd |> Map.get(:message_set)

    assert got_message1.key == message1.key
    assert got_message1.value == message1.value
    assert got_message1.offset == offset
    assert got_message2.key == message2.key
    assert got_message2.value == message2.value
    assert got_message2.offset == offset + 1
  end

  # larger messages
  test "publish/fetch handles a 10kb message", %{client: client} do
    topic = "large_message_test"

    # 10 chars * 1024 repeats ~= 10kb
    message_value = String.duplicate("ABCDEFGHIJ", 1024)
    messages = [%Proto.Produce.Message{key: nil, value: message_value}]

    produce_request = %Proto.Produce.Request{
      topic: topic,
      partition: 0,
      required_acks: 1,
      messages: messages
    }

    {:ok, offset} = KafkaEx.produce(produce_request, worker_name: client)

    fetch_response = KafkaEx.fetch(topic, 0, offset: offset, worker_name: client) |> hd

    [got_message] = fetch_response.partitions |> hd |> Map.get(:message_set)

    assert nil == got_message.key
    assert message_value == got_message.value
  end

  test "publish/fetch handles a 10kb message snappy compressed", %{
    client: client
  } do
    topic = "large_message_test"

    # 10 chars * 1024 repeats ~= 10kb
    message_value = String.duplicate("ABCDEFGHIJ", 100)
    messages = [%Proto.Produce.Message{key: nil, value: message_value}]

    produce_request = %Proto.Produce.Request{
      topic: topic,
      partition: 0,
      compression: :snappy,
      required_acks: 1,
      messages: messages
    }

    {:ok, offset} = KafkaEx.produce(produce_request, worker_name: client)

    fetch_response = KafkaEx.fetch(topic, 0, offset: offset, worker_name: client) |> hd

    [got_message] = fetch_response.partitions |> hd |> Map.get(:message_set)

    assert nil == got_message.key
    assert message_value == got_message.value
  end

  # stream
  test "streams kafka logs", %{client: client} do
    topic_name = TestHelper.generate_random_string()

    KafkaEx.produce(
      %Proto.Produce.Request{
        topic: topic_name,
        partition: 0,
        required_acks: 1,
        messages: [
          %Proto.Produce.Message{value: "message 1"},
          %Proto.Produce.Message{value: "message 2"},
          %Proto.Produce.Message{value: "message 3"},
          %Proto.Produce.Message{value: "message 4"}
        ]
      },
      worker_name: client
    )

    stream =
      KafkaEx.stream(
        topic_name,
        0,
        worker_name: client,
        offset: 0,
        auto_commit: false
      )

    logs = Enum.take(stream, 2)
    assert 2 == length(logs)
    [m1, m2] = logs
    assert m1.value == "message 1"
    assert m2.value == "message 2"

    # calling stream again will get the same messages
    assert logs == Enum.take(stream, 2)
  end

  test "stream with small max_bytes makes multiple requests if necessary", %{
    client: client
  } do
    topic_name = TestHelper.generate_random_string()

    KafkaEx.produce(
      %Proto.Produce.Request{
        topic: topic_name,
        partition: 0,
        required_acks: 1,
        messages: [
          %Proto.Produce.Message{value: "message 1"},
          %Proto.Produce.Message{value: "message 2"},
          %Proto.Produce.Message{value: "message 3"},
          %Proto.Produce.Message{value: "message 4"},
          %Proto.Produce.Message{value: "message 5"}
        ]
      },
      worker_name: client
    )

    stream =
      KafkaEx.stream(
        topic_name,
        0,
        worker_name: client,
        offset: 0,
        auto_commit: false
      )

    logs = Enum.take(stream, 4)
    assert 4 == length(logs)
    [m1, m2, m3, m4] = logs
    assert m1.value == "message 1"
    assert m2.value == "message 2"
    assert m3.value == "message 3"
    assert m4.value == "message 4"

    # calling stream again will get the same messages
    assert logs == Enum.take(stream, 4)
  end

  test "stream blocks until new messages are available", %{client: client} do
    topic_name = TestHelper.generate_random_string()

    KafkaEx.produce(
      %Proto.Produce.Request{
        topic: topic_name,
        partition: 0,
        required_acks: 1,
        messages: [
          %Proto.Produce.Message{value: "message 1"}
        ]
      },
      worker_name: client
    )

    stream =
      KafkaEx.stream(
        topic_name,
        0,
        worker_name: client,
        offset: 0,
        auto_commit: false
      )

    task = Task.async(fn -> Enum.take(stream, 4) end)

    KafkaEx.produce(
      %Proto.Produce.Request{
        topic: topic_name,
        partition: 0,
        required_acks: 1,
        messages: [
          %Proto.Produce.Message{value: "message 2"},
          %Proto.Produce.Message{value: "message 3"},
          %Proto.Produce.Message{value: "message 4"},
          %Proto.Produce.Message{value: "message 5"}
        ]
      },
      worker_name: client
    )

    logs = Task.await(task)

    assert 4 == length(logs)
    [m1, m2, m3, m4] = logs
    assert m1.value == "message 1"
    assert m2.value == "message 2"
    assert m3.value == "message 3"
    assert m4.value == "message 4"
  end

  test "stream is non-blocking with no_wait_at_logend", %{client: client} do
    topic_name = TestHelper.generate_random_string()

    KafkaEx.produce(
      %Proto.Produce.Request{
        topic: topic_name,
        partition: 0,
        required_acks: 1,
        messages: [
          %Proto.Produce.Message{value: "message 1"}
        ]
      },
      worker_name: client
    )

    stream =
      KafkaEx.stream(
        topic_name,
        0,
        worker_name: client,
        offset: 0,
        auto_commit: false,
        no_wait_at_logend: true
      )

    logs = Enum.take(stream, 4)

    assert 1 == length(logs)
    [m1] = logs
    assert m1.value == "message 1"

    # we can also execute something like 'map' which would otherwise be
    # open-ended
    assert ["message 1"] == Enum.map(stream, fn m -> m.value end)
  end

  test "doesn't error when re-creating an existing stream", %{client: client} do
    random_string = TestHelper.generate_random_string()
    KafkaEx.stream(random_string, 0, offset: 0, worker_name: client)
    KafkaEx.stream(random_string, 0, offset: 0, worker_name: client)
  end

  test "produce with the default partitioner works", %{client: client} do
    topic = TestHelper.generate_random_string()
    :ok = KafkaEx.produce(topic, nil, "hello", worker_name: client)
  end
end
