defmodule KafkaEx.Integration.Test do
  alias KafkaEx.Protocol, as: Proto
  use ExUnit.Case
  import TestHelper

  @moduletag :integration

  test "KafkaEx.Server starts on Application start up" do
    pid = Process.whereis(KafkaEx.Server)
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
    metadata_update_interval = :sys.get_state(pid).metadata_update_interval

    assert metadata_update_interval == 10
  end

  test "create_worker provides a default metadata_update_interval of '30000'" do
    {:ok, pid} = KafkaEx.create_worker(:d, uris: uris)
    metadata_update_interval = :sys.get_state(pid).metadata_update_interval

    assert metadata_update_interval == 30000
  end

  test "create_worker allows custom consumer_group_update_interval" do
    {:ok, pid} = KafkaEx.create_worker(:consumer_group_update_interval_custom, uris: uris, consumer_group_update_interval: 10)
    consumer_group_update_interval = :sys.get_state(pid).consumer_group_update_interval

    assert consumer_group_update_interval == 10
  end

  test "create_worker provides a default consumer_group_update_interval of '30000'" do
    {:ok, pid} = KafkaEx.create_worker(:de, uris: uris)
    consumer_group_update_interval = :sys.get_state(pid).consumer_group_update_interval

    assert consumer_group_update_interval == 30000
  end

  test "create_worker provides a default consumer_group of 'kafka_ex'" do
    {:ok, pid} = KafkaEx.create_worker(:baz, uris: uris)
    consumer_group = :sys.get_state(pid).consumer_group

    assert consumer_group == "kafka_ex"
  end

  test "create_worker takes a consumer_group option and sets that as the consumer_group of the worker" do
    {:ok, pid} = KafkaEx.create_worker(:joe, [uris: uris, consumer_group: "foo"])
    consumer_group = :sys.get_state(pid).consumer_group

    assert consumer_group == "foo"
  end

  #update_metadata
  test "worker updates metadata after specified interval" do
    random_string = generate_random_string
    KafkaEx.create_worker(:update_metadata, [uris: uris, consumer_group: "foo", metadata_update_interval: 100])
    previous_metadata = KafkaEx.metadata(worker_name: :update_metadata)
    KafkaEx.produce(%Proto.Produce.Request{topic: random_string, required_acks: 0, messages: [%Proto.Produce.Message{value: "hey"}]})
    :timer.sleep(105)
    new_metadata = KafkaEx.metadata(worker_name: :update_metadata)

    refute previous_metadata == new_metadata
  end


  test "KafkaEx.Server generates metadata on start up" do
    pid = Process.whereis(KafkaEx.Server)
    KafkaEx.produce("food", 0, "hey", worker_name: KafkaEx.Server, required_acks: 1)
    metadata = :sys.get_state(pid).metadata

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
    produce_response = KafkaEx.produce("food", 0, "hey", worker_name: KafkaEx.Server, required_acks: 1) |> hd
    offset = produce_response.partitions |> hd |> Map.get(:offset)

    refute offset == nil
  end

  test "produce without an acq required returns :ok" do
    assert KafkaEx.produce(%Proto.Produce.Request{topic: "food", required_acks: 0, messages: [%Proto.Produce.Message{value: "hey"}]}) == :ok
  end

  test "produce with ack required returns an ack" do
    produce_response = KafkaEx.produce(%Proto.Produce.Request{topic: "food", required_acks: 1, messages: [%Proto.Produce.Message{value: "hey"}]}) |> hd
    offset = produce_response.partitions |> hd |> Map.get(:offset)

    refute offset == nil
  end

  test "produce updates metadata" do
    {:ok, pid} = KafkaEx.create_worker(:update_metadata_test)
    empty_metadata = %Proto.Metadata.Response{}
    :sys.replace_state(pid, fn(state) -> %{state | metadata: empty_metadata} end)

    assert empty_metadata.brokers == []

    KafkaEx.produce(%Proto.Produce.Request{topic: "food", required_acks: 1, messages: [%Proto.Produce.Message{value: "hey"}]}, worker_name: :update_metadata_test)
    metadata = :sys.get_state(pid).metadata

    refute metadata == empty_metadata
    refute metadata.brokers == []
  end

  test "produce creates log for a non-existing topic" do
    random_string = generate_random_string
    KafkaEx.produce(%Proto.Produce.Request{topic: random_string, required_acks: 1, messages: [%Proto.Produce.Message{value: "hey"}]})
    pid = Process.whereis(KafkaEx.Server)
    metadata = :sys.get_state(pid).metadata

    assert Enum.find_value(metadata.topic_metadatas, &(&1.topic == random_string))
  end

  #metadata
  test "metadata works" do
    random_string = generate_random_string
    Enum.each(1..10, fn _ -> KafkaEx.produce(%Proto.Produce.Request{topic: random_string, required_acks: 1, messages: [%Proto.Produce.Message{value: "hey"}]}) end)

    refute Enum.empty?(Enum.flat_map(KafkaEx.metadata.topic_metadatas, fn(metadata) -> metadata.partition_metadatas end))
  end

  test "metadata for a non-existing topic creates a new topic" do
    random_string = generate_random_string
    random_topic_metadata = Enum.find(KafkaEx.metadata(topic: random_string).topic_metadatas, &(&1.topic == random_string))

    refute random_topic_metadata.partition_metadatas == []
    assert Enum.all?(random_topic_metadata.partition_metadatas, &(&1.error_code == 0))

    pid = Process.whereis(KafkaEx.Server)
    metadata = :sys.get_state(pid).metadata
    random_topic_metadata = Enum.find(metadata.topic_metadatas, &(&1.topic == random_string))

    refute random_topic_metadata.partition_metadatas == []
    assert Enum.all?(random_topic_metadata.partition_metadatas, &(&1.error_code == 0))
  end

  #fetch
  test "fetch updates metadata" do
    {:ok, pid} = KafkaEx.create_worker(:fetch_updates_metadata)
    empty_metadata = %Proto.Metadata.Response{}
    :sys.replace_state(pid, fn(state) -> %{state | :metadata => empty_metadata} end)
    KafkaEx.fetch("food", 0, offset: 0, auto_commit: false, worker_name: :fetch_updates_metadata)
    metadata = :sys.get_state(pid).metadata

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
    produce_response =  KafkaEx.produce(%Proto.Produce.Request{topic: random_string, required_acks: 1, messages: [%Proto.Produce.Message{value: "hey foo"}]}) |> hd
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
    :sys.replace_state(pid, fn(state) -> %{state | :metadata => empty_metadata} end)
    KafkaEx.offset("food", 0, utc_time, :offset_updates_metadata)
    metadata = :sys.get_state(pid).metadata

    refute metadata == empty_metadata
  end

  test "offset retrieves most recent offset by time specification" do
    random_string = generate_random_string
    KafkaEx.produce(%Proto.Produce.Request{topic: random_string, required_acks: 1, messages: [%Proto.Produce.Message{value: "hey"}]})
    offset_response = KafkaEx.offset(random_string, 0, utc_time) |> hd
    offset = offset_response.partition_offsets |> hd |> Map.get(:offset) |> hd

    assert offset != 0
  end

  test "earliest_offset retrieves offset of 0" do
    random_string = generate_random_string
    KafkaEx.produce(%Proto.Produce.Request{topic: random_string, required_acks: 1, messages: [%Proto.Produce.Message{value: "hey"}]})
    offset_response = KafkaEx.earliest_offset(random_string, 0) |> hd
    offset = offset_response.partition_offsets |> hd |> Map.get(:offset) |> hd

    assert offset == 0
  end

  test "latest_offset retrieves offset of 0 for non-existing topic" do
    random_string = generate_random_string
    produce_offset = KafkaEx.produce(%Proto.Produce.Request{topic: random_string, required_acks: 1, messages: [%Proto.Produce.Message{value: "hey"}]}) |> hd |> Map.get(:partitions) |> hd |> Map.get(:offset)
    offset_response = KafkaEx.latest_offset(random_string, 0) |> hd
    offset = offset_response.partition_offsets |> hd |> Map.get(:offset) |> hd

    assert offset == produce_offset + 1
  end

  test "latest_offset retrieves a non-zero offset for a topic published to" do
    random_string = generate_random_string
    KafkaEx.produce(%Proto.Produce.Request{topic: random_string, required_acks: 1, messages: [%Proto.Produce.Message{value: "foo"}]})
    offset_response = KafkaEx.latest_offset(random_string, 0) |> hd
    offset = offset_response.partition_offsets |> hd |> Map.get(:offset) |> hd

    assert offset != 0
  end

  # stream
  test "streams kafka logs" do
    random_string = generate_random_string
    KafkaEx.create_worker(:stream, uris: uris)
    KafkaEx.produce(%Proto.Produce.Request{topic: random_string, required_acks: 1, messages: [
        %Proto.Produce.Message{value: "hey"},
        %Proto.Produce.Message{value: "hi"},
      ]
    }, worker_name: :stream)
    
    stream = KafkaEx.stream(random_string, 0, worker_name: :stream, offset: 0, auto_commit: false)
    :timer.sleep(100)
    log = GenEvent.call(stream.manager, KafkaExHandler, :messages)

    refute Enum.empty?(log)
    [first,second|_] = log
    assert first.value == "hey"
    assert second.value == "hi"
  end

  test "stop_streaming stops streaming, and stream starts it up again" do
    random_string = generate_random_string
    KafkaEx.create_worker(:stream2, uris: uris)
    stream = KafkaEx.stream(random_string, 0, worker_name: :stream2, offset: 0, auto_commit: false)

    KafkaEx.create_worker(:producer, uris: uris)
    KafkaEx.produce(%Proto.Produce.Request{topic: random_string, required_acks: 1, messages: [
        %Proto.Produce.Message{value: "one"},
        %Proto.Produce.Message{value: "two"},
      ]
    }, worker_name: :producer)

    :timer.sleep(1000)
    log = GenEvent.call(stream.manager, KafkaExHandler, :messages)
    assert length(log) == 2
    last_offset = hd(Enum.reverse(log)).offset

    KafkaEx.stop_streaming(worker_name: :stream2)
    :timer.sleep(1000)
    KafkaEx.produce(%Proto.Produce.Request{topic: random_string, required_acks: 1, messages: [
        %Proto.Produce.Message{value: "three"},
        %Proto.Produce.Message{value: "four"},
      ]
    }, worker_name: :producer)
    :timer.sleep(1000)
    stream = KafkaEx.stream(random_string, 0, worker_name: :stream2, offset: last_offset+1, auto_commit: false)
    log = GenEvent.call(stream.manager, KafkaExHandler, :messages)
    assert length(log) == 0

    KafkaEx.produce(%Proto.Produce.Request{topic: random_string, required_acks: 1, messages: [
        %Proto.Produce.Message{value: "five"},
        %Proto.Produce.Message{value: "six"},
      ]
    }, worker_name: :producer)
    :timer.sleep(1000)
    log = GenEvent.call(stream.manager, KafkaExHandler, :messages)
    assert length(log) == 4
  end
end
