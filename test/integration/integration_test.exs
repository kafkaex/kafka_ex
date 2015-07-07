defmodule KafkaEx.Integration.Test do
  alias KafkaEx.Protocol, as: Proto
  use ExUnit.Case
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

  test "KafkaEx.Server generates metadata on start up" do
    pid = Process.whereis(KafkaEx.Server)
    metadata = :sys.get_state(pid).metadata

    refute metadata == %Proto.Metadata.Response{}
    refute metadata.brokers == []
  end

  test "start_link creates the server and registers it as the module name" do
    {:ok, pid} = KafkaEx.create_worker(:test_server, uris: uris)
    assert pid == Process.whereis(:test_server)
  end

  #produce
  test "produce without an acq required returns :ok" do
    assert KafkaEx.produce("food", 0, "hey") == :ok
  end

  test "produce with ack required returns an ack" do
    produce_response = KafkaEx.produce("food", 0, "hey", worker_name: KafkaEx.Server, required_acks: 1) |> hd
    offset = produce_response.partitions |> hd |> Map.get(:offset)

    refute offset == nil
  end

  test "produce updates metadata" do
    pid = Process.whereis(KafkaEx.Server)
    empty_metadata = %Proto.Metadata.Response{}
    :sys.replace_state(pid, fn(state) -> %{state | metadata: empty_metadata} end)

    assert empty_metadata.brokers == []

    KafkaEx.produce("food", 0, "hey")
    metadata = :sys.get_state(pid).metadata

    refute metadata == empty_metadata
    refute metadata.brokers == []
  end

  test "produce creates log for a non-existing topic" do
    random_string = TestHelper.generate_random_string
    KafkaEx.produce(random_string, 0, "hey")
    pid = Process.whereis(KafkaEx.Server)
    metadata = :sys.get_state(pid).metadata

    assert Enum.find_value(metadata.topic_metadatas, &(&1.topic == random_string))
  end

  #metadata
  test "metadata works" do
    random_string = TestHelper.generate_random_string
    Enum.each(1..10, fn _ -> KafkaEx.produce(random_string, 0, "hey foo", worker_name: KafkaEx.Server, required_acks: 1) end)

    refute Enum.empty?(Enum.flat_map(KafkaEx.metadata.topic_metadatas, fn(metadata) -> metadata.partition_metadatas end))
  end

  test "metadata for a non-existing topic creates a new topic" do
    random_string = TestHelper.generate_random_string
    random_topic_metadata = Enum.find(KafkaEx.metadata(topic: random_string).topic_metadatas, &(&1.topic == random_string))

    refute random_topic_metadata.partition_metadatas == []
    assert Enum.all?(random_topic_metadata.partition_metadatas, &(&1.error_code == 0))

    pid = Process.whereis(KafkaEx.Server)
    metadata = :sys.get_state(pid).metadata
    random_topic_metadata = Enum.find(metadata.topic_metadatas, &(&1.topic == random_string))

    refute random_topic_metadata.partition_metadatas == []
    assert Enum.all?(random_topic_metadata.partition_metadatas, &(&1.error_code == 0))
  end

  test "consumer_group_metadata works" do
    random_string = TestHelper.generate_random_string
    pid = Process.whereis(KafkaEx.Server)
    metadata = KafkaEx.consumer_group_metadata(KafkaEx.Server, random_string)
    consumer_group_metadata = :sys.get_state(pid).consumer_metadata

    assert metadata != %Proto.ConsumerMetadata.Response{}
    assert metadata.coordinator_host != nil
    assert metadata.error_code == 0
    assert metadata == consumer_group_metadata
  end

  #fetch
  test "fetch updates metadata" do
    pid = Process.whereis(KafkaEx.Server)
    empty_metadata = %Proto.Metadata.Response{}
    :sys.replace_state(pid, fn(state) -> %{state | :metadata => empty_metadata} end)
    KafkaEx.fetch("food", 0, offset: 0)
    metadata = :sys.get_state(pid).metadata

    refute metadata == empty_metadata
  end

  test "fetch auto_commits offset by default" do
    random_string = TestHelper.generate_random_string
    KafkaEx.create_worker(:fetch_test_worker)
    Enum.each(1..10, fn _ -> KafkaEx.produce(random_string, 0, "hey foo", worker_name: KafkaEx.Server, required_acks: 1) end)
    offset = KafkaEx.fetch(random_string, 0, offset: 0, worker: :fetch_test_worker) |> hd |> Map.get(:partitions) |> hd |> Map.get(:message_set) |> Enum.reverse |> hd |> Map.get(:offset)

    offset_fetch_response = KafkaEx.offset_fetch(:fetch_test_worker, %Proto.OffsetFetch.Request{topic: random_string}) |> hd
    error_code = offset_fetch_response.partitions |> hd |> Map.get(:error_code)
    offset_fetch_response_offset = offset_fetch_response.partitions |> hd |> Map.get(:offset)

    assert error_code == 0
    assert offset == offset_fetch_response_offset
  end

  test "fetch starts consuming from last committed offset" do
    random_string = TestHelper.generate_random_string
    KafkaEx.create_worker(:fetch_test_committed_worker)
    Enum.each(1..10, fn _ -> KafkaEx.produce(random_string, 0, "hey foo", worker_name: KafkaEx.Server, required_acks: 1) end)
    KafkaEx.offset_commit(:fetch_test_committed_worker, %Proto.OffsetCommit.Request{topic: random_string, offset: 3})
    logs = KafkaEx.fetch(random_string, 0, worker: :fetch_test_committed_worker) |> hd |> Map.get(:partitions) |> hd |> Map.get(:message_set)

    first_message = logs |> hd

    assert first_message.offset == 3
    assert length(logs) == 7
  end

  test "fetch does not commit offset with auto_commit is set to false" do
    KafkaEx.create_worker(:fetch_no_auto_commit_worker)
    Enum.each(1..10, fn _ -> KafkaEx.produce("food", 0, "hey foo", worker_name: KafkaEx.Server, required_acks: 1) end)
    offset = KafkaEx.fetch("food", 0, offset: 0, worker: :fetch_no_auto_commit_worker, auto_commit: false) |> hd |> Map.get(:partitions) |> hd |> Map.get(:message_set) |> Enum.reverse |> hd |> Map.get(:offset)

    offset_fetch_response = KafkaEx.offset_fetch(:fetch_no_auto_commit_worker, %Proto.OffsetFetch.Request{topic: "food"}) |> hd
    error_code = offset_fetch_response.partitions |> hd |> Map.get(:error_code)
    offset_fetch_response_offset = offset_fetch_response.partitions |> hd |> Map.get(:offset)

    assert error_code == 0
    refute offset == offset_fetch_response_offset
  end

  test "fetch does not blow up with incomplete bytes" do
    KafkaEx.fetch("food", 0, offset: 0, max_bytes: 100)
  end

  test "fetch returns ':topic_not_found' for non-existing topic" do
    random_string = TestHelper.generate_random_string

    assert KafkaEx.fetch(random_string, 0, offset: 0) == :topic_not_found
  end

  test "fetch works" do
    random_string = TestHelper.generate_random_string
    produce_response =  KafkaEx.produce(random_string, 0, "hey foo", worker_name: KafkaEx.Server, required_acks: 1) |> hd
    offset = produce_response.partitions |> hd |> Map.get(:offset)
    fetch_response = KafkaEx.fetch(random_string, 0, offset: 0) |>  hd
    message = fetch_response.partitions |> hd |> Map.get(:message_set) |> hd

    assert message.value == "hey foo"
    assert message.offset == offset
  end

  #offset
  test "offset updates metadata" do
    pid = Process.whereis(KafkaEx.Server)
    empty_metadata = %Proto.Metadata.Response{}
    :sys.replace_state(pid, fn(state) -> %{state | :metadata => empty_metadata} end)
    KafkaEx.offset("food", 0, utc_time)
    metadata = :sys.get_state(pid).metadata

    refute metadata == empty_metadata
  end

  test "offset retrieves most recent offset by time specification" do
    random_string = TestHelper.generate_random_string
    KafkaEx.produce(random_string, 0, "hey")
    offset_response = KafkaEx.offset(random_string, 0, utc_time) |> hd
    offset = offset_response.partition_offsets |> hd |> Map.get(:offset) |> hd

    assert offset != 0
  end

  test "earliest_offset retrieves offset of 0" do
    random_string = TestHelper.generate_random_string
    KafkaEx.produce(random_string, 0, "hey")
    offset_response = KafkaEx.earliest_offset(random_string, 0) |> hd
    offset = offset_response.partition_offsets |> hd |> Map.get(:offset) |> hd

    assert offset == 0
  end

  test "latest_offset retrieves offset of 0 for non-existing topic" do
    random_string = TestHelper.generate_random_string
    produce_offset = KafkaEx.produce(random_string, 0, "hey", required_acks: 1) |> hd |> Map.get(:partitions) |> hd |> Map.get(:offset)
    offset_response = KafkaEx.latest_offset(random_string, 0) |> hd
    offset = offset_response.partition_offsets |> hd |> Map.get(:offset) |> hd

    assert offset == produce_offset + 1
  end

  test "offset_commit commits an offset and offset_fetch retrieves the committed offset" do
    random_string = TestHelper.generate_random_string
    Enum.each(1..10, fn _ -> KafkaEx.produce(random_string, 0, "foo") end)
    assert KafkaEx.offset_commit(KafkaEx.Server, %Proto.OffsetCommit.Request{topic: random_string, offset: 9}) ==
      [%Proto.OffsetCommit.Response{partitions: [0], topic: random_string}]
    assert KafkaEx.offset_fetch(KafkaEx.Server, %Proto.OffsetFetch.Request{topic: random_string}) ==
      [%Proto.OffsetFetch.Response{partitions: [%{metadata: "", error_code: 0, offset: 9, partition: 0}], topic: random_string}]
  end

  test "latest_offset retrieves a non-zero offset for a topic published to" do
    random_string = TestHelper.generate_random_string
    KafkaEx.produce(random_string, 0, "foo")
    offset_response = KafkaEx.latest_offset(random_string, 0) |> hd
    offset = offset_response.partition_offsets |> hd |> Map.get(:offset) |> hd

    assert offset != 0
  end

  # stream
  test "streams kafka logs" do
    random_string = TestHelper.generate_random_string
    KafkaEx.create_worker(:stream, uris: uris)
    KafkaEx.produce(random_string, 0, "hey", worker_name: :stream)
    KafkaEx.produce(random_string, 0, "hi", worker_name: :stream)
    log = KafkaEx.stream(random_string, 0, worker_name: :stream) |> Enum.take(2)

    refute Enum.empty?(log)
    [first,second|_] = log
    assert first.value == "hey"
    assert second.value == "hi"
  end

  test "stream auto_commits offset by default" do
    random_string = TestHelper.generate_random_string
    KafkaEx.create_worker(:stream_auto_commit, uris: uris)
    KafkaEx.produce(random_string, 0, "hey", required_acks: 1)
    KafkaEx.produce(random_string, 0, "hi", required_acks: 1)
    log = KafkaEx.stream(random_string, 0, worker_name: :stream_auto_commit) |> Enum.take(2)

    refute Enum.empty?(log)

    offset_fetch_response = KafkaEx.offset_fetch(:stream_auto_commit, %Proto.OffsetFetch.Request{topic: random_string}) |> hd
    error_code = offset_fetch_response.partitions |> hd |> Map.get(:error_code)
    offset = offset_fetch_response.partitions |> hd |> Map.get(:offset)

    assert error_code == 0
    refute offset == 0
  end

  test "stream starts consuming from last committed offset" do
    random_string = TestHelper.generate_random_string
    KafkaEx.create_worker(:stream_last_committed_offset, uris: uris)
    Enum.each(1..10, fn _ -> KafkaEx.produce(random_string, 0, "hey foo", required_acks: 1) end)
    KafkaEx.offset_commit(:stream_last_committed_offset, %Proto.OffsetCommit.Request{topic: random_string, offset: 3})
    log = KafkaEx.stream(random_string, 0, worker_name: :stream_last_committed_offset) |> Enum.take(2)

    refute Enum.empty?(log)

    first_message = log |> hd

    assert first_message.offset == 3
  end

  test "stream does not commit offset with auto_commit is set to false" do
    random_string = TestHelper.generate_random_string
    KafkaEx.create_worker(:stream_no_auto_commit, uris: uris)
    Enum.each(1..10, fn _ -> KafkaEx.produce(random_string, 0, "hey foo", required_acks: 1) end)
    KafkaEx.stream(random_string, 0, worker_name: :stream_no_auto_commit, auto_commit: false) |> Enum.take(2)

    offset_fetch_response = KafkaEx.offset_fetch(:stream_no_auto_commit, %Proto.OffsetFetch.Request{topic: random_string}) |> hd
    offset_fetch_response.partitions |> hd |> Map.get(:error_code)
    offset_fetch_response_offset = offset_fetch_response.partitions |> hd |> Map.get(:offset)

    assert 0 >= offset_fetch_response_offset
  end

  test "stop_streaming stops streaming, and stream starts it up again" do
    random_string = TestHelper.generate_random_string
    KafkaEx.create_worker(:stream2, uris: uris)
    stream = KafkaEx.stream(random_string, 0, worker_name: :stream2)

    KafkaEx.create_worker(:producer, uris: uris)
    KafkaEx.produce(random_string, 0, "one", worker_name: :producer, required_acks: 1)
    KafkaEx.produce(random_string, 0, "two", worker_name: :producer, required_acks: 1)

    :timer.sleep(1000)
    log = GenEvent.call(stream.manager, KafkaExHandler, :messages)
    assert length(log) == 2
    last_offset = hd(Enum.reverse(log)).offset

    KafkaEx.stop_streaming(worker_name: :stream2)
    :timer.sleep(1000)
    KafkaEx.produce(random_string, 0, "three", worker_name: :producer, required_acks: 1)
    KafkaEx.produce(random_string, 0, "four", worker_name: :producer, required_acks: 1)
    :timer.sleep(1000)
    stream = KafkaEx.stream(random_string, 0, worker_name: :stream2, offset: last_offset+1)
    log = GenEvent.call(stream.manager, KafkaExHandler, :messages)
    assert length(log) == 0

    KafkaEx.produce(random_string, 0, "five", worker_name: :producer, required_acks: 1)
    KafkaEx.produce(random_string, 0, "six", worker_name: :producer, required_acks: 1)
    :timer.sleep(1000)
    log = GenEvent.call(stream.manager, KafkaExHandler, :messages)
    assert length(log) == 4
  end

  def uris do
    Mix.Config.read!("config/config.exs") |> hd |> elem(1) |> hd |> elem(1)
  end

  def utc_time do
    {x, {a,b,c}} = :calendar.local_time |> :calendar.local_time_to_universal_time_dst |> hd
    {x, {a,b,c + 60}}
  end
end
