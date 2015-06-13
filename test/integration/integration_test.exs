defmodule KafkaEx.Integration.Test do
  use ExUnit.Case
  @moduletag :integration

  test "KafkaEx.Server starts on Application start up" do
    pid = Process.whereis(KafkaEx.Server)
    assert is_pid(pid)
  end

  #create_worker
  test "KafkaEx.Supervisor dynamically creates workers" do
    {:ok, pid} = KafkaEx.create_worker(:bar, uris)
    assert Process.whereis(:bar) == pid
  end

  test "KafkaEx.Server generates metadata on start up" do
    pid = Process.whereis(KafkaEx.Server)
    metadata = :sys.get_state(pid).metadata
    refute metadata == %{}

    brokers = Map.values(metadata[:brokers])

    assert Enum.sort(brokers) == Enum.sort(uris)
  end

  test "start_link creates the server and registers it as the module name" do
    {:ok, pid} = KafkaEx.create_worker(:test_server, uris)
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
    empty_metadata = KafkaEx.Metadata.new(uris)
    :sys.replace_state(pid, fn(state) -> %{state | metadata: empty_metadata} end)
    KafkaEx.produce("food", 0, "hey")
    metadata = :sys.get_state(pid).metadata
    refute metadata == empty_metadata

    brokers = Map.values(metadata[:brokers])

    assert Enum.sort(brokers) == Enum.sort(uris)
  end

  test "produce creates log for a non-existing topic" do
    random_string = TestHelper.generate_random_string
    KafkaEx.produce(random_string, 0, "hey")
    pid = Process.whereis(KafkaEx.Server)
    metadata = :sys.get_state(pid).metadata
    random_topic_metadata_found = metadata[:topics] |> Map.keys |> Enum.member?(random_string)

    assert random_topic_metadata_found
  end

  #metadata
  test "metadata for a non-existing topic creates a new topic" do
    random_string = TestHelper.generate_random_string
    random_topic_metadata = KafkaEx.metadata(topic: random_string)[:topics][random_string]
    assert random_topic_metadata[:error_code] == :no_error
    refute random_topic_metadata[:partitions] == %{}

    pid = Process.whereis(KafkaEx.Server)
    metadata = :sys.get_state(pid).metadata
    random_topic_metadata_found = metadata[:topics] |> Map.keys |> Enum.member?(random_string)

    assert random_topic_metadata_found
  end

  test "consumer_group_metadata works" do
    random_string = TestHelper.generate_random_string
    pid = Process.whereis(KafkaEx.Server)
    metadata = KafkaEx.consumer_group_metadata(KafkaEx.Server, random_string)
    consumer_group_metadata = :sys.get_state(pid).consumer_metadata

    assert metadata != %KafkaEx.Protocol.ConsumerMetadata.Response{}
    assert metadata.coordinator_host != nil
    assert metadata.error_code == 0
    assert metadata == consumer_group_metadata
  end

  #fetch
  test "fetch updates metadata" do
    pid = Process.whereis(KafkaEx.Server)
    empty_metadata = KafkaEx.Metadata.new(uris)
    :sys.replace_state(pid, fn(state) -> %{state | :metadata => empty_metadata} end)
    KafkaEx.fetch("food", 0, 0)
    metadata = :sys.get_state(pid).metadata
    refute metadata == empty_metadata

    brokers = Map.values(metadata[:brokers])

    assert Enum.sort(brokers) == Enum.sort(uris)
  end

  test "fetch does not blow up with incomplete bytes" do
    KafkaEx.fetch("food", 0, 0, max_bytes: 100)
  end

  test "fetch retrieves empty logs for non-exisiting topic" do
    random_string = TestHelper.generate_random_string
    fetch_response = KafkaEx.fetch(random_string, 0, 0) |> hd
    message_set = fetch_response.partitions |> hd |> Map.get(:message_set)

    assert message_set == []
  end

  test "fetch works" do
    random_string = TestHelper.generate_random_string
    produce_response =  KafkaEx.produce(random_string, 0, "hey foo", worker_name: KafkaEx.Server, required_acks: 1) |> hd
    offset = produce_response.partitions |> hd |> Map.get(:offset)
    fetch_response = KafkaEx.fetch(random_string, 0, 0) |>  hd
    message = fetch_response.partitions |> hd |> Map.get(:message_set) |> hd

    assert message.value == "hey foo"
    assert message.offset == offset
  end

  #offset
  test "offset updates metadata" do
    pid = Process.whereis(KafkaEx.Server)
    empty_metadata = KafkaEx.Metadata.new(uris)
    :sys.replace_state(pid, fn(state) -> %{state | :metadata => empty_metadata} end)
    KafkaEx.offset("food", 0, utc_time)
    metadata = :sys.get_state(pid).metadata
    refute metadata == empty_metadata

    brokers = Map.values(metadata[:brokers])

    assert Enum.sort(brokers) == Enum.sort(uris)
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
    offset_response = KafkaEx.latest_offset(random_string, 0) |> hd
    offset = offset_response.partition_offsets |> hd |> Map.get(:offset) |> hd

    assert offset == 0
  end

  test "latest_offset retrieves offset of 0 for non-existing topic" do
    random_string = TestHelper.generate_random_string
    offset_response = KafkaEx.latest_offset(random_string, 0) |> hd
    offset = offset_response.partition_offsets |> hd |> Map.get(:offset) |> hd

    assert offset == 0
  end

  test "offset_commit commits an offset and offset_fetch retrieves the committed offset" do
    random_string = TestHelper.generate_random_string
    Enum.each(1..10, fn _ -> KafkaEx.produce(random_string, 0, "foo") end)
    assert KafkaEx.offset_commit(KafkaEx.Server, %KafkaEx.Protocol.OffsetCommit.Request{topic: random_string, offset: 9}) ==  
      [%KafkaEx.Protocol.OffsetCommit.Response{partitions: [0], topic: random_string}]
    assert KafkaEx.offset_fetch(KafkaEx.Server, %KafkaEx.Protocol.OffsetFetch.Request{topic: random_string}) == 
      [%KafkaEx.Protocol.OffsetFetch.Response{partitions: [%{metadata: "", error_code: 0, offset: 9, partition: 0}], topic: random_string}]
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
    KafkaEx.create_worker(:stream, uris)
    KafkaEx.produce(random_string, 0, "hey", worker_name: :stream)
    KafkaEx.produce(random_string, 0, "hi", worker_name: :stream)
    log = KafkaEx.stream(random_string, 0, worker_name: :stream) |> Enum.take(2)

    refute Enum.empty?(log)
    [first,second|_] = log
    assert first.value == "hey"
    assert second.value == "hi"
  end

  test "stop_streaming stops streaming, and stream starts it up again" do
    random_string = TestHelper.generate_random_string
    KafkaEx.create_worker(:stream2, uris)
    stream = KafkaEx.stream(random_string, 0, worker_name: :stream2)

    KafkaEx.create_worker(:producer, uris)
    KafkaEx.produce(random_string, 0, "one", worker_name: :producer)
    KafkaEx.produce(random_string, 0, "two", worker_name: :producer)

    :timer.sleep(1000)
    log = GenEvent.call(stream.manager, KafkaExHandler, :messages)
    assert length(log) == 2
    last_offset = hd(Enum.reverse(log)).offset

    KafkaEx.stop_streaming(worker_name: :stream2)
    :timer.sleep(1000)
    KafkaEx.produce(random_string, 0, "three", worker_name: :producer)
    KafkaEx.produce(random_string, 0, "four", worker_name: :producer)
    :timer.sleep(1000)
    log = GenEvent.call(stream.manager, KafkaExHandler, :messages)
    assert length(log) == 0

    stream = KafkaEx.stream(random_string, 0, worker_name: :stream2, offset: last_offset+1)
    KafkaEx.produce(random_string, 0, "five", worker_name: :producer)
    KafkaEx.produce(random_string, 0, "six", worker_name: :producer)
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
