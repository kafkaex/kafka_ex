defmodule KafkaEx.Integration.Test do
  alias KafkaEx.Protocol, as: Proto
  alias KafkaEx.Config
  use ExUnit.Case
  import TestHelper

  @moduletag :integration

  test "default worker starts on Application start up" do
    pid = Process.whereis(Config.default_worker())
    assert is_pid(pid)
  end

  # create_worker
  test "KafkaEx.Supervisor dynamically creates workers" do
    {:ok, pid} = KafkaEx.create_worker(:bar, uris: uris())
    assert Process.whereis(:bar) == pid
  end

  test "Creates a worker even when the one of the provided brokers is not available" do
    {:ok, pid} =
      KafkaEx.create_worker(
        :no_broker_worker,
        uris: uris() ++ [{"bad_host", 9000}]
      )

    assert Process.whereis(:no_broker_worker) == pid
  end

  test "create_worker allows custom metadata_update_interval" do
    {:ok, pid} =
      KafkaEx.create_worker(
        :metadata_update_interval_custom,
        uris: uris(),
        metadata_update_interval: 10
      )

    metadata_update_interval = :sys.get_state(pid).metadata_update_interval

    assert metadata_update_interval == 10
  end

  test "create_worker provides a default metadata_update_interval of '30000'" do
    {:ok, pid} = KafkaEx.create_worker(:d, uris: uris())
    metadata_update_interval = :sys.get_state(pid).metadata_update_interval

    assert metadata_update_interval == 30000
  end

  # update_metadata
  test "worker updates metadata after specified interval" do
    random_string = generate_random_string()

    KafkaEx.create_worker(
      :update_metadata,
      uris: uris(),
      consumer_group: "foo",
      metadata_update_interval: 100
    )

    previous_metadata = KafkaEx.metadata(worker_name: :update_metadata)

    KafkaEx.produce(%Proto.Produce.Request{
      topic: random_string,
      partition: 0,
      required_acks: 0,
      messages: [%Proto.Produce.Message{value: "hey"}]
    })

    :timer.sleep(105)
    new_metadata = KafkaEx.metadata(worker_name: :update_metadata)

    refute previous_metadata == new_metadata
  end

  test "default worker generates metadata on start up" do
    pid = Process.whereis(Config.default_worker())

    KafkaEx.produce(
      "food",
      0,
      "hey",
      worker_name: Config.default_worker(),
      required_acks: 1
    )

    metadata = :sys.get_state(pid).metadata

    refute metadata == %Proto.Metadata.Response{}
    refute metadata.brokers == []
  end

  test "start_link creates the server and registers it as the module name" do
    {:ok, pid} = KafkaEx.create_worker(:test_server, uris: uris())
    assert pid == Process.whereis(:test_server)
  end

  # produce
  test "produce/4 without an acq required returns :ok" do
    assert KafkaEx.produce("food", 0, "hey") == :ok
  end

  test "produce/4 with ack required returns an ack" do
    {:ok, offset} =
      KafkaEx.produce(
        "food",
        0,
        "hey",
        worker_name: Config.default_worker(),
        required_acks: 1
      )

    refute offset == nil
  end

  test "produce without an acq required returns :ok" do
    assert KafkaEx.produce(%Proto.Produce.Request{
             topic: "food",
             partition: 0,
             required_acks: 0,
             messages: [%Proto.Produce.Message{value: "hey"}]
           }) == :ok
  end

  test "produce with ack required returns an ack" do
    {:ok, offset} =
      KafkaEx.produce(%Proto.Produce.Request{
        topic: "food",
        partition: 0,
        required_acks: 1,
        messages: [%Proto.Produce.Message{value: "hey"}]
      })

    refute offset == nil
  end

  test "produce updates metadata" do
    {:ok, pid} = KafkaEx.create_worker(:update_metadata_test)
    empty_metadata = %Proto.Metadata.Response{}

    :sys.replace_state(pid, fn state ->
      %{state | :metadata => empty_metadata}
    end)

    assert empty_metadata.brokers == []

    KafkaEx.produce(
      %Proto.Produce.Request{
        topic: "food",
        partition: 0,
        required_acks: 1,
        messages: [%Proto.Produce.Message{value: "hey"}]
      },
      worker_name: :update_metadata_test
    )

    metadata = :sys.get_state(pid).metadata

    refute metadata == empty_metadata
    refute metadata.brokers == []
  end

  test "produce creates log for a non-existing topic" do
    random_string = generate_random_string()

    KafkaEx.produce(%Proto.Produce.Request{
      topic: random_string,
      partition: 0,
      required_acks: 1,
      messages: [%Proto.Produce.Message{value: "hey"}]
    })

    pid = Process.whereis(Config.default_worker())
    metadata = :sys.get_state(pid).metadata

    assert Enum.find_value(
             metadata.topic_metadatas,
             &(&1.topic == random_string)
           )
  end

  # metadata
  test "metadata works" do
    random_string = generate_random_string()

    Enum.each(1..10, fn _ ->
      KafkaEx.produce(%Proto.Produce.Request{
        topic: random_string,
        partition: 0,
        required_acks: 1,
        messages: [%Proto.Produce.Message{value: "hey"}]
      })
    end)

    refute Enum.empty?(
             Enum.flat_map(KafkaEx.metadata().topic_metadatas, fn metadata ->
               metadata.partition_metadatas
             end)
           )
  end

  test "metadata for a non-existing topic creates a new topic" do
    random_string = generate_random_string()

    metadata =
      TestHelper.wait_for_value(
        fn -> KafkaEx.metadata(topic: random_string) end,
        fn metadata ->
          metadata != nil && length(metadata.topic_metadatas) > 0
        end
      )

    random_topic_metadata =
      Enum.find(metadata.topic_metadatas, &(&1.topic == random_string))

    refute random_topic_metadata.partition_metadatas == []

    assert Enum.all?(
             random_topic_metadata.partition_metadatas,
             &(&1.error_code == :no_error)
           )

    pid = Process.whereis(Config.default_worker())
    metadata = :sys.get_state(pid).metadata

    random_topic_metadata =
      Enum.find(metadata.topic_metadatas, &(&1.topic == random_string))

    refute random_topic_metadata.partition_metadatas == []

    assert Enum.all?(
             random_topic_metadata.partition_metadatas,
             &(&1.error_code == :no_error)
           )
  end

  # fetch
  test "fetch updates metadata" do
    {:ok, pid} = KafkaEx.create_worker(:fetch_updates_metadata)
    empty_metadata = %Proto.Metadata.Response{}

    :sys.replace_state(pid, fn state ->
      %{state | :metadata => empty_metadata}
    end)

    KafkaEx.fetch(
      "food",
      0,
      offset: 0,
      auto_commit: false,
      worker_name: :fetch_updates_metadata
    )

    :timer.sleep(200)
    metadata = :sys.get_state(pid).metadata

    refute metadata == empty_metadata
  end

  test "fetch does not blow up with incomplete bytes" do
    KafkaEx.fetch("food", 0, offset: 0, max_bytes: 100, auto_commit: false)
  end

  test "fetch returns ':topic_not_found' for non-existing topic" do
    random_string = generate_random_string()

    assert KafkaEx.fetch(random_string, 0, offset: 0) == :topic_not_found
  end

  test "fetch works" do
    random_string = generate_random_string()

    {:ok, offset} =
      KafkaEx.produce(%Proto.Produce.Request{
        topic: random_string,
        partition: 0,
        required_acks: 1,
        messages: [%Proto.Produce.Message{value: "hey foo"}]
      })

    fetch_response =
      KafkaEx.fetch(random_string, 0, offset: 0, auto_commit: false) |> hd

    message = fetch_response.partitions |> hd |> Map.get(:message_set) |> hd

    assert message.value == "hey foo"
    assert message.offset == offset
  end

  # offset
  test "offset updates metadata" do
    {:ok, pid} = KafkaEx.create_worker(:offset_updates_metadata)
    empty_metadata = %Proto.Metadata.Response{}

    :sys.replace_state(pid, fn state ->
      %{state | :metadata => empty_metadata}
    end)

    KafkaEx.offset("food", 0, utc_time(), :offset_updates_metadata)
    metadata = :sys.get_state(pid).metadata

    refute metadata == empty_metadata
  end

  test "offset retrieves most recent offset by time specification" do
    random_string = generate_random_string()

    KafkaEx.produce(%Proto.Produce.Request{
      topic: random_string,
      partition: 0,
      required_acks: 1,
      messages: [%Proto.Produce.Message{value: "hey"}]
    })

    offset_response = KafkaEx.offset(random_string, 0, utc_time()) |> hd
    offset = offset_response.partition_offsets |> hd |> Map.get(:offset) |> hd

    assert offset != 0
  end

  test "earliest_offset retrieves offset of 0" do
    random_string = generate_random_string()

    KafkaEx.produce(%Proto.Produce.Request{
      topic: random_string,
      partition: 0,
      required_acks: 1,
      messages: [%Proto.Produce.Message{value: "hey"}]
    })

    offset_response = KafkaEx.earliest_offset(random_string, 0) |> hd
    offset = offset_response.partition_offsets |> hd |> Map.get(:offset) |> hd

    assert offset == 0
  end

  test "latest_offset retrieves offset of 0 for non-existing topic" do
    random_string = generate_random_string()

    {:ok, produce_offset} =
      KafkaEx.produce(%Proto.Produce.Request{
        topic: random_string,
        partition: 0,
        required_acks: 1,
        messages: [%Proto.Produce.Message{value: "hey"}]
      })

    :timer.sleep(300)
    offset_response = KafkaEx.latest_offset(random_string, 0) |> hd
    offset = offset_response.partition_offsets |> hd |> Map.get(:offset) |> hd

    assert offset == produce_offset + 1
  end

  test "latest_offset retrieves a non-zero offset for a topic published to" do
    random_string = generate_random_string()

    KafkaEx.produce(%Proto.Produce.Request{
      topic: random_string,
      partition: 0,
      required_acks: 1,
      messages: [%Proto.Produce.Message{value: "foo"}]
    })

    [offset_response] =
      TestHelper.wait_for_any(fn -> KafkaEx.latest_offset(random_string, 0) end)

    offset = offset_response.partition_offsets |> hd |> Map.get(:offset) |> hd

    assert offset != 0
  end

  # compression
  test "compresses / decompresses using gzip" do
    random_string = generate_random_string()

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

    {:ok, offset} = KafkaEx.produce(produce_request)

    fetch_response =
      KafkaEx.fetch(random_string, 0, offset: 0, auto_commit: false) |> hd

    [got_message1, got_message2] =
      fetch_response.partitions |> hd |> Map.get(:message_set)

    assert got_message1.key == message1.key
    assert got_message1.value == message1.value
    assert got_message1.offset == offset
    assert got_message2.key == message2.key
    assert got_message2.value == message2.value
    assert got_message2.offset == offset + 1
  end

  test "compresses / decompresses using snappy" do
    random_string = generate_random_string()

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

    {:ok, offset} = KafkaEx.produce(produce_request)

    fetch_response =
      KafkaEx.fetch(random_string, 0, offset: 0, auto_commit: false) |> hd

    [got_message1, got_message2] =
      fetch_response.partitions |> hd |> Map.get(:message_set)

    assert got_message1.key == message1.key
    assert got_message1.value == message1.value
    assert got_message1.offset == offset
    assert got_message2.key == message2.key
    assert got_message2.value == message2.value
    assert got_message2.offset == offset + 1
  end

  # larger messages
  test "publish/fetch handles a 10kb message" do
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

    {:ok, offset} = KafkaEx.produce(produce_request)

    fetch_response = KafkaEx.fetch(topic, 0, offset: offset) |> hd
    [got_message] = fetch_response.partitions |> hd |> Map.get(:message_set)

    assert nil == got_message.key
    assert message_value == got_message.value
  end

  test "publish/fetch handles a 10kb message gzip compressed" do
    topic = "large_message_test_gzip"

    # 10 chars * 1024 repeats ~= 10kb
    message_value = String.duplicate("ABCDEFGHIJ", 100)
    messages = [%Proto.Produce.Message{key: nil, value: message_value}]

    produce_request = %Proto.Produce.Request{
      topic: topic,
      partition: 0,
      compression: :gzip,
      required_acks: 1,
      messages: messages
    }

    {:ok, offset} = KafkaEx.produce(produce_request)

    fetch_response = KafkaEx.fetch(topic, 0, offset: offset) |> hd
    [got_message] = fetch_response.partitions |> hd |> Map.get(:message_set)

    assert nil == got_message.key
    assert message_value == got_message.value
  end

  test "publish/fetch handles a 10kb message snappy compressed" do
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

    {:ok, offset} = KafkaEx.produce(produce_request)

    fetch_response = KafkaEx.fetch(topic, 0, offset: offset) |> hd
    [got_message] = fetch_response.partitions |> hd |> Map.get(:message_set)

    assert nil == got_message.key
    assert message_value == got_message.value
  end

  # stream
  test "streams kafka logs" do
    topic_name = generate_random_string()
    KafkaEx.create_worker(:stream, uris: uris())

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
      worker_name: :stream
    )

    stream =
      KafkaEx.stream(
        topic_name,
        0,
        worker_name: :stream,
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

  test "stream with small max_bytes makes multiple requests if necessary" do
    topic_name = generate_random_string()
    KafkaEx.create_worker(:stream, uris: uris())

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
      worker_name: :stream
    )

    stream =
      KafkaEx.stream(
        topic_name,
        0,
        worker_name: :stream,
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

  test "stream blocks until new messages are available" do
    topic_name = generate_random_string()
    KafkaEx.create_worker(:stream, uris: uris())

    KafkaEx.produce(
      %Proto.Produce.Request{
        topic: topic_name,
        partition: 0,
        required_acks: 1,
        messages: [
          %Proto.Produce.Message{value: "message 1"}
        ]
      },
      worker_name: :stream
    )

    stream =
      KafkaEx.stream(
        topic_name,
        0,
        worker_name: :stream,
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
      worker_name: :stream
    )

    logs = Task.await(task)

    assert 4 == length(logs)
    [m1, m2, m3, m4] = logs
    assert m1.value == "message 1"
    assert m2.value == "message 2"
    assert m3.value == "message 3"
    assert m4.value == "message 4"
  end

  test "stream is non-blocking with no_wait_at_logend" do
    topic_name = generate_random_string()
    KafkaEx.create_worker(:stream, uris: uris())

    KafkaEx.produce(
      %Proto.Produce.Request{
        topic: topic_name,
        partition: 0,
        required_acks: 1,
        messages: [
          %Proto.Produce.Message{value: "message 1"}
        ]
      },
      worker_name: :stream
    )

    stream =
      KafkaEx.stream(
        topic_name,
        0,
        worker_name: :stream,
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

  test "doesn't error when re-creating an existing stream" do
    random_string = generate_random_string()
    KafkaEx.stream(random_string, 0, offset: 0)
    KafkaEx.stream(random_string, 0, offset: 0)
  end
end
