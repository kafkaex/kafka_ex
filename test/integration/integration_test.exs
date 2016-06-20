defmodule KafkaEx.Integration.Test do
  alias KafkaEx.Protocol, as: Proto
  use ExUnit.Case
  import TestHelper

  @moduletag :integration

  test "KafkaEx.server starts on Application start up" do
    pid = Process.whereis(KafkaEx.server)
    assert is_pid(pid)
  end

  #create_worker
  test "KafkaEx.Supervisor dynamically creates workers" do
    {:ok, pid} = KafkaEx.create_worker(:bar, uris: uris)
    assert Process.whereis(:bar) == pid
  end

  test "Creates a worker even when the one of the provided brokers is not available" do
    {:ok, pid} = KafkaEx.create_worker(:no_broker_worker, uris: uris ++ [{"bad_host", 9000}])
    assert Process.whereis(:no_broker_worker) == pid
  end

  test "create_worker allows custom metadata_update_interval" do
    {:ok, pid} = KafkaEx.create_worker(:metadata_update_interval_custom, uris: uris, metadata_update_interval: 10)
    metadata_update_interval = :sys.get_state(pid).callback_state.metadata_update_interval

    assert metadata_update_interval == 10
  end

  test "create_worker provides a default metadata_update_interval of '30000'" do
    {:ok, pid} = KafkaEx.create_worker(:d, uris: uris)
    metadata_update_interval = :sys.get_state(pid).callback_state.metadata_update_interval

    assert metadata_update_interval == 30000
  end

  test "create_worker provides a default sync_timeout of 1000 if not set in config" do
    value_before = Application.get_env(:kafka_ex, :sync_timeout)
    Application.delete_env(:kafka_ex, :sync_timeout)
    {:ok, pid} = KafkaEx.create_worker(:bif, uris: uris)
    sync_timeout = :sys.get_state(pid).callback_state.sync_timeout

    assert sync_timeout == 1000
    Application.put_env(:kafka_ex, :sync_timeout, value_before)
  end

  test "create_worker takes a sync_timeout option and sets that as the sync_timeout of the worker" do
    {:ok, pid} = KafkaEx.create_worker(:babar, [uris: uris, sync_timeout: 2000])
    sync_timeout = :sys.get_state(pid).callback_state.sync_timeout

    assert sync_timeout == 2000
  end

  test "create_worker uses sync_timeout default from config if set" do
    value_before = Application.get_env(:kafka_ex, :sync_timeout)
    Application.put_env(:kafka_ex, :sync_timeout, 4000)

    {:ok, pid} = KafkaEx.create_worker(:eve, [uris: uris])
    sync_timeout = :sys.get_state(pid).callback_state.sync_timeout

    assert sync_timeout == 4000
    Application.put_env(:kafka_ex, :sync_timeout, value_before)
  end

  test "create_worker uses explicit sync_timeout even if set in config" do
    {:ok, pid} = KafkaEx.create_worker(:alice, [uris: uris, sync_timeout: 2000])
    sync_timeout = :sys.get_state(pid).callback_state.sync_timeout

    assert sync_timeout == 2000
  end

  #update_metadata
  test "worker updates metadata after specified interval" do
    random_string = generate_random_string
    KafkaEx.create_worker(:update_metadata, [uris: uris, consumer_group: "foo", metadata_update_interval: 100])
    previous_metadata = KafkaEx.metadata(worker_name: :update_metadata)
    KafkaEx.produce(%Proto.Produce.Request{topic: random_string, partition: 0, required_acks: 0, messages: [%Proto.Produce.Message{value: "hey"}]})
    :timer.sleep(105)
    new_metadata = KafkaEx.metadata(worker_name: :update_metadata)

    refute previous_metadata == new_metadata
  end


  test "KafkaEx.server generates metadata on start up" do
    pid = Process.whereis(KafkaEx.server)
    KafkaEx.produce("food", 0, "hey", worker_name: KafkaEx.server, required_acks: 1)
    metadata = :sys.get_state(pid).callback_state.metadata

    refute metadata == %Proto.Metadata.Response{}
    refute metadata.brokers == []
  end

  test "start_link creates the server and registers it as the module name" do
    {:ok, pid} = KafkaEx.create_worker(:test_server, uris: uris)
    assert pid == Process.whereis(:test_server)
  end

  #produce
  test "produce/4 without an acq required returns :ok" do
    assert KafkaEx.produce("food", 0, "hey") == :ok
  end

  test "produce/4 with ack required returns an ack" do
    produce_response = KafkaEx.produce("food", 0, "hey", worker_name: KafkaEx.server, required_acks: 1) |> hd
    offset = produce_response.partitions |> hd |> Map.get(:offset)

    refute offset == nil
  end

  test "produce without an acq required returns :ok" do
    assert KafkaEx.produce(%Proto.Produce.Request{topic: "food", partition: 0, required_acks: 0, messages: [%Proto.Produce.Message{value: "hey"}]}) == :ok
  end

  test "produce with ack required returns an ack" do
    produce_response = KafkaEx.produce(%Proto.Produce.Request{topic: "food", partition: 0, required_acks: 1, messages: [%Proto.Produce.Message{value: "hey"}]}) |> hd
    offset = produce_response.partitions |> hd |> Map.get(:offset)

    refute offset == nil
  end

  test "produce updates metadata" do
    {:ok, pid} = KafkaEx.create_worker(:update_metadata_test)
    empty_metadata = %Proto.Metadata.Response{}
    :sys.replace_state(pid, fn(state) ->
      callback_state = state.callback_state
      new_callback_state = %{callback_state | :metadata => empty_metadata}
      %{state | :callback_state => new_callback_state}
    end)

    assert empty_metadata.brokers == []

    KafkaEx.produce(%Proto.Produce.Request{topic: "food", partition: 0, required_acks: 1, messages: [%Proto.Produce.Message{value: "hey"}]}, worker_name: :update_metadata_test)
    metadata = :sys.get_state(pid).callback_state.metadata

    refute metadata == empty_metadata
    refute metadata.brokers == []
  end

  test "produce creates log for a non-existing topic" do
    random_string = generate_random_string
    KafkaEx.produce(%Proto.Produce.Request{topic: random_string, partition: 0, required_acks: 1, messages: [%Proto.Produce.Message{value: "hey"}]})
    pid = Process.whereis(KafkaEx.server)
    metadata = :sys.get_state(pid).callback_state.metadata

    assert Enum.find_value(metadata.topic_metadatas, &(&1.topic == random_string))
  end

  #metadata
  test "metadata works" do
    random_string = generate_random_string
    Enum.each(1..10, fn _ -> KafkaEx.produce(%Proto.Produce.Request{topic: random_string, partition: 0, required_acks: 1, messages: [%Proto.Produce.Message{value: "hey"}]}) end)

    refute Enum.empty?(Enum.flat_map(KafkaEx.metadata.topic_metadatas, fn(metadata) -> metadata.partition_metadatas end))
  end

  test "metadata for a non-existing topic creates a new topic" do
    random_string = generate_random_string
    metadata = TestHelper.wait_for_value(
      fn() -> KafkaEx.metadata(topic: random_string) end,
      fn(metadata) -> metadata != nil && length(metadata.topic_metadatas) > 0 end
    )
    random_topic_metadata = Enum.find(metadata.topic_metadatas, &(&1.topic == random_string))

    refute random_topic_metadata.partition_metadatas == []
    assert Enum.all?(random_topic_metadata.partition_metadatas, &(&1.error_code == :no_error))

    pid = Process.whereis(KafkaEx.server)
    metadata = :sys.get_state(pid).callback_state.metadata
    random_topic_metadata = Enum.find(metadata.topic_metadatas, &(&1.topic == random_string))

    refute random_topic_metadata.partition_metadatas == []
    assert Enum.all?(random_topic_metadata.partition_metadatas, &(&1.error_code == :no_error))
  end

  #fetch
  test "fetch updates metadata" do
    {:ok, pid} = KafkaEx.create_worker(:fetch_updates_metadata)
    empty_metadata = %Proto.Metadata.Response{}
    :sys.replace_state(pid, fn(state) ->
      callback_state = state.callback_state
      new_callback_state = %{callback_state | :metadata => empty_metadata}
      %{state | :callback_state => new_callback_state}
    end)
    KafkaEx.fetch("food", 0, offset: 0, auto_commit: false, worker_name: :fetch_updates_metadata)
    :timer.sleep(200)
    metadata = :sys.get_state(pid).callback_state.metadata

    refute metadata == empty_metadata
  end

  test "fetch does not blow up with incomplete bytes" do
    KafkaEx.fetch("food", 0, offset: 0, max_bytes: 100, auto_commit: false)
  end

  test "fetch returns ':topic_not_found' for non-existing topic" do
    random_string = generate_random_string

    assert KafkaEx.fetch(random_string, 0, offset: 0) == :topic_not_found
  end

  test "fetch works" do
    random_string = generate_random_string
    produce_response =  KafkaEx.produce(%Proto.Produce.Request{topic: random_string, partition: 0, required_acks: 1, messages: [%Proto.Produce.Message{value: "hey foo"}]}) |> hd
    offset = produce_response.partitions |> hd |> Map.get(:offset)
    fetch_response = KafkaEx.fetch(random_string, 0, offset: 0, auto_commit: false) |>  hd
    message = fetch_response.partitions |> hd |> Map.get(:message_set) |> hd

    assert message.value == "hey foo"
    assert message.offset == offset
  end

  #offset
  test "offset updates metadata" do
    {:ok, pid} = KafkaEx.create_worker(:offset_updates_metadata)
    empty_metadata = %Proto.Metadata.Response{}
    :sys.replace_state(pid, fn(state) ->
      callback_state = state.callback_state
      new_callback_state = %{callback_state | :metadata => empty_metadata}
      %{state | :callback_state => new_callback_state}
    end)
    KafkaEx.offset("food", 0, utc_time, :offset_updates_metadata)
    metadata = :sys.get_state(pid).callback_state.metadata

    refute metadata == empty_metadata
  end

  test "offset retrieves most recent offset by time specification" do
    random_string = generate_random_string
    KafkaEx.produce(%Proto.Produce.Request{topic: random_string, partition: 0, required_acks: 1, messages: [%Proto.Produce.Message{value: "hey"}]})
    offset_response = KafkaEx.offset(random_string, 0, utc_time) |> hd
    offset = offset_response.partition_offsets |> hd |> Map.get(:offset) |> hd

    assert offset != 0
  end

  test "earliest_offset retrieves offset of 0" do
    random_string = generate_random_string
    KafkaEx.produce(%Proto.Produce.Request{topic: random_string, partition: 0, required_acks: 1, messages: [%Proto.Produce.Message{value: "hey"}]})
    offset_response = KafkaEx.earliest_offset(random_string, 0) |> hd
    offset = offset_response.partition_offsets |> hd |> Map.get(:offset) |> hd

    assert offset == 0
  end

  test "latest_offset retrieves offset of 0 for non-existing topic" do
    random_string = generate_random_string
    produce_offset = KafkaEx.produce(%Proto.Produce.Request{topic: random_string, partition: 0, required_acks: 1, messages: [%Proto.Produce.Message{value: "hey"}]}) |> hd |> Map.get(:partitions) |> hd |> Map.get(:offset)
    :timer.sleep(300)
    offset_response = KafkaEx.latest_offset(random_string, 0) |> hd
    offset = offset_response.partition_offsets |> hd |> Map.get(:offset) |> hd

    assert offset == produce_offset + 1
  end

  test "latest_offset retrieves a non-zero offset for a topic published to" do
    random_string = generate_random_string
    KafkaEx.produce(%Proto.Produce.Request{topic: random_string, partition: 0, required_acks: 1, messages: [%Proto.Produce.Message{value: "foo"}]})
    [offset_response] = TestHelper.wait_for_any(
      fn() -> KafkaEx.latest_offset(random_string, 0) end
    )
    offset = offset_response.partition_offsets |> hd |> Map.get(:offset) |> hd

    assert offset != 0
  end

  # compression
  test "compresses / decompresses using gzip" do
    random_string = generate_random_string

    message1 = %Proto.Produce.Message{value: "value 1"}
    message2 = %Proto.Produce.Message{key: "key 2", value: "value 2"}
    messages = [message1, message2]

    produce_request = %Proto.Produce.Request{
      topic: random_string,
      partition: 0,
      required_acks: 1,
      compression: :gzip,
      messages: messages}
    produce_response =  KafkaEx.produce(produce_request) |> hd
    offset = produce_response.partitions |> hd |> Map.get(:offset)

    fetch_response = KafkaEx.fetch(random_string, 0, offset: 0, auto_commit: false) |>  hd
    [got_message1, got_message2] = fetch_response.partitions |> hd |> Map.get(:message_set)

    assert got_message1.key == message1.key
    assert got_message1.value == message1.value
    assert got_message1.offset == offset
    assert got_message2.key == message2.key
    assert got_message2.value == message2.value
    assert got_message2.offset == offset + 1
  end

  test "compresses / decompresses using snappy" do
    random_string = generate_random_string

    message1 = %Proto.Produce.Message{value: "value 1"}
    message2 = %Proto.Produce.Message{key: "key 2", value: "value 2"}
    messages = [message1, message2]

    produce_request = %Proto.Produce.Request{
      topic: random_string,
      partition: 0,
      required_acks: 1,
      compression: :snappy,
      messages: messages}
    produce_response =  KafkaEx.produce(produce_request) |> hd
    offset = produce_response.partitions |> hd |> Map.get(:offset)

    fetch_response = KafkaEx.fetch(random_string, 0, offset: 0, auto_commit: false) |>  hd
    [got_message1, got_message2] = fetch_response.partitions |> hd |> Map.get(:message_set)

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
    produce_request = %Proto.Produce.Request{topic: topic, partition: 0, required_acks: 1, messages: messages}

    produce_response = KafkaEx.produce(produce_request) |> hd
    offset = produce_response.partitions |> hd |> Map.get(:offset)

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
    produce_request = %Proto.Produce.Request{topic: topic, partition: 0, compression: :gzip, required_acks: 1, messages: messages}

    produce_response = KafkaEx.produce(produce_request) |> hd
    offset = produce_response.partitions |> hd |> Map.get(:offset)

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
    produce_request = %Proto.Produce.Request{topic: topic, partition: 0, compression: :snappy, required_acks: 1, messages: messages}

    produce_response = KafkaEx.produce(produce_request) |> hd
    offset = produce_response.partitions |> hd |> Map.get(:offset)

    fetch_response = KafkaEx.fetch(topic, 0, offset: offset) |> hd
    [got_message] = fetch_response.partitions |> hd |> Map.get(:message_set)

    assert nil == got_message.key
    assert message_value == got_message.value
  end

  # stream
  test "streams kafka logs" do
    random_string = generate_random_string
    KafkaEx.create_worker(:stream, uris: uris)
    KafkaEx.produce(%Proto.Produce.Request{topic: random_string, partition: 0, required_acks: 1, messages: [
        %Proto.Produce.Message{value: "hey"},
        %Proto.Produce.Message{value: "hi"},
      ]
    }, worker_name: :stream)

    stream = KafkaEx.stream(random_string, 0, worker_name: :stream, offset: 0, auto_commit: false)
    log = TestHelper.wait_for_accum(
      fn() -> GenEvent.call(stream.manager, KafkaEx.Handler, :messages) end,
      2
    )

    refute Enum.empty?(log)
    [first,second|_] = log
    assert first.value == "hey"
    assert second.value == "hi"
  end

  test "doesn't error when re-creating an existing stream" do
    random_string = generate_random_string
    KafkaEx.stream(random_string, 0, offset: 0)
    KafkaEx.stream(random_string, 0, offset: 0)
  end

  test "stop_streaming stops streaming, and stream starts it up again" do
    random_string = generate_random_string
    KafkaEx.create_worker(:stream2, uris: uris)
    stream = KafkaEx.stream(random_string, 0, worker_name: :stream2, offset: 0, auto_commit: false)

    KafkaEx.create_worker(:producer, uris: uris)
    KafkaEx.produce(%Proto.Produce.Request{topic: random_string, partition: 0, required_acks: 1, messages: [
        %Proto.Produce.Message{value: "one"},
        %Proto.Produce.Message{value: "two"},
      ]
    }, worker_name: :producer)

    log = TestHelper.wait_for_accum(
      fn() -> GenEvent.call(stream.manager, KafkaEx.Handler, :messages) end,
      2
    )

    last_offset = hd(Enum.reverse(log)).offset

    KafkaEx.stop_streaming(worker_name: :stream2)
    :ok = TestHelper.wait_for(fn() -> !Process.alive?(stream.manager) end)

    KafkaEx.produce(%Proto.Produce.Request{topic: random_string, partition: 0, required_acks: 1, messages: [
        %Proto.Produce.Message{value: "three"},
        %Proto.Produce.Message{value: "four"},
      ]
    }, worker_name: :producer)

    stream = KafkaEx.stream(random_string, 0, worker_name: :stream2, offset: last_offset+1, auto_commit: false)

    :ok = TestHelper.wait_for(fn() -> Process.alive?(stream.manager) end)

    log = GenEvent.call(stream.manager, KafkaEx.Handler, :messages)
    assert length(log) == 0

    KafkaEx.produce(%Proto.Produce.Request{topic: random_string, partition: 0, required_acks: 1, messages: [
        %Proto.Produce.Message{value: "five"},
        %Proto.Produce.Message{value: "six"},
      ]
    }, worker_name: :producer)

    log = TestHelper.wait_for_accum(
      fn() -> GenEvent.call(stream.manager, KafkaEx.Handler, :messages) end,
      4
    )

    assert length(log) == 4
  end

  test "streams kafka logs with custom handler and initial state" do
    random_string = generate_random_string
    KafkaEx.create_worker(:stream3, uris: uris)
    produce_response = KafkaEx.produce(%Proto.Produce.Request{topic: random_string, partition: 0, required_acks: 1, messages: [
        %Proto.Produce.Message{value: "hey"},
        %Proto.Produce.Message{value: "hi"},
      ]
    }, worker_name: :stream3) |> hd
    offset = produce_response.partitions |> hd |> Map.get(:offset)

    defmodule CustomHandlerSendingMessage do
      use GenEvent

      def init(pid) do
        {:ok, pid}
      end

      def handle_event(message, pid) do
        send(pid, message)
        {:ok, pid}
      end
    end
    KafkaEx.stream(random_string, 0, worker_name: :stream3, offset: offset, auto_commit: false, handler: CustomHandlerSendingMessage, handler_init: self())

    assert_receive %KafkaEx.Protocol.Fetch.Message{key: nil, value: "hey", offset: ^offset, attributes: 0, crc: 4264455069}
    offset = offset + 1
    assert_receive %KafkaEx.Protocol.Fetch.Message{key: nil, value: "hi", offset: ^offset, attributes: 0, crc: 4251893211}
    receive do
      _ -> assert(false, "Should not have received a third message")
      after 100 -> :ok
    end
  end
end
