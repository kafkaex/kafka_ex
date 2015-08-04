defmodule KafkaEx.ConsumerGroup.Test do
  alias KafkaEx.Protocol, as: Proto
  use ExUnit.Case
  import TestHelper

  @moduletag :consumer_group

  test "consumer_group_metadata works" do
    random_string = generate_random_string
    pid = Process.whereis(KafkaEx.Server)
    KafkaEx.produce(%Proto.Produce.Request{topic: "food", required_acks: 1, messages: [%Proto.Produce.Message{value: "hey"}]})
    KafkaEx.fetch("food", 0, offset: 0)
    metadata = KafkaEx.consumer_group_metadata(KafkaEx.Server, random_string)
    consumer_group_metadata = :sys.get_state(pid).consumer_metadata

    assert metadata != %Proto.ConsumerMetadata.Response{}
    assert metadata.coordinator_host != nil
    assert metadata.error_code == 0
    assert metadata == consumer_group_metadata
  end

  #update_consumer_metadata
  test "worker updates metadata after specified interval" do
    {:ok, pid} = KafkaEx.create_worker(:update_consumer_metadata, [uris: uris, consumer_group: "kafka_ex", consumer_group_update_interval: 100])
    consumer_metadata = %KafkaEx.Protocol.ConsumerMetadata.Response{}
    :sys.replace_state(pid, fn(state) -> %{state | consumer_metadata: consumer_metadata} end)
    :timer.sleep(105)
    new_consumer_metadata = :sys.get_state(pid).consumer_metadata

    refute new_consumer_metadata == consumer_metadata
  end

  test "worker does not update metadata when consumer_group is false" do
    {:ok, pid} = KafkaEx.create_worker(:no_consumer_metadata_update, [uris: uris, consumer_group: false, consumer_group_update_interval: 100])
    consumer_metadata = %KafkaEx.Protocol.ConsumerMetadata.Response{}
    :sys.replace_state(pid, fn(state) -> %{state | consumer_metadata: consumer_metadata} end)
    :timer.sleep(105)
    new_consumer_metadata = :sys.get_state(pid).consumer_metadata

    assert new_consumer_metadata == consumer_metadata
  end

  #fetch
  test "fetch auto_commits offset by default" do
    random_string = generate_random_string
    KafkaEx.create_worker(:fetch_test_worker)
    Enum.each(1..10, fn _ -> KafkaEx.produce(%Proto.Produce.Request{topic: random_string, required_acks: 1, messages: [%Proto.Produce.Message{value: "hey"}]}) end)
    offset = KafkaEx.fetch(random_string, 0, offset: 0, worker: :fetch_test_worker) |> hd |> Map.get(:partitions) |> hd |> Map.get(:message_set) |> Enum.reverse |> hd |> Map.get(:offset)

    offset_fetch_response = KafkaEx.offset_fetch(:fetch_test_worker, %Proto.OffsetFetch.Request{topic: random_string}) |> hd
    error_code = offset_fetch_response.partitions |> hd |> Map.get(:error_code)
    offset_fetch_response_offset = offset_fetch_response.partitions |> hd |> Map.get(:offset)

    assert error_code == 0
    assert offset == offset_fetch_response_offset
  end

  test "fetch starts consuming from last committed offset" do
    random_string = generate_random_string
    KafkaEx.create_worker(:fetch_test_committed_worker)
    Enum.each(1..10, fn _ -> KafkaEx.produce(%Proto.Produce.Request{topic: random_string, required_acks: 1, messages: [%Proto.Produce.Message{value: "hey"}]}) end)
    KafkaEx.offset_commit(:fetch_test_committed_worker, %Proto.OffsetCommit.Request{topic: random_string, offset: 3})
    logs = KafkaEx.fetch(random_string, 0, worker: :fetch_test_committed_worker) |> hd |> Map.get(:partitions) |> hd |> Map.get(:message_set)

    first_message = logs |> hd

    assert first_message.offset == 3
    assert length(logs) == 7
  end

  test "fetch does not commit offset with auto_commit is set to false" do
    KafkaEx.create_worker(:fetch_no_auto_commit_worker)
    Enum.each(1..10, fn _ -> KafkaEx.produce(%Proto.Produce.Request{topic: "food", required_acks: 1, messages: [%Proto.Produce.Message{value: "hey"}]}) end)
    offset = KafkaEx.fetch("food", 0, offset: 0, worker: :fetch_no_auto_commit_worker, auto_commit: false) |> hd |> Map.get(:partitions) |> hd |> Map.get(:message_set) |> Enum.reverse |> hd |> Map.get(:offset)
    offset_fetch_response = KafkaEx.offset_fetch(:fetch_no_auto_commit_worker, %Proto.OffsetFetch.Request{topic: "food"}) |> hd
    offset_fetch_response_offset = offset_fetch_response.partitions |> hd |> Map.get(:offset)

    refute offset == offset_fetch_response_offset
  end

  #offset_commit
  test "offset_commit commits an offset and offset_fetch retrieves the committed offset" do
    random_string = generate_random_string
    Enum.each(1..10, fn _ -> KafkaEx.produce(%Proto.Produce.Request{topic: random_string, required_acks: 1, messages: [%Proto.Produce.Message{value: "hey"}]}) end)
    assert KafkaEx.offset_commit(KafkaEx.Server, %Proto.OffsetCommit.Request{topic: random_string, offset: 9}) ==
      [%Proto.OffsetCommit.Response{partitions: [0], topic: random_string}]
    assert KafkaEx.offset_fetch(KafkaEx.Server, %Proto.OffsetFetch.Request{topic: random_string}) ==
      [%Proto.OffsetFetch.Response{partitions: [%{metadata: "", error_code: 0, offset: 9, partition: 0}], topic: random_string}]
  end

  #stream
  test "stream auto_commits offset by default" do
    random_string = generate_random_string
    KafkaEx.create_worker(:stream_auto_commit, uris: uris)
    KafkaEx.produce(%Proto.Produce.Request{topic: random_string, required_acks: 1, messages: [
        %Proto.Produce.Message{value: "hey"},
        %Proto.Produce.Message{value: "hi"},
      ]
    })
    :timer.sleep(500)
    log = KafkaEx.stream(random_string, 0, worker_name: :stream_auto_commit, offset: 0) |> Enum.take(2)

    refute Enum.empty?(log)

    offset_fetch_response = KafkaEx.offset_fetch(:stream_auto_commit, %Proto.OffsetFetch.Request{topic: random_string}) |> hd
    error_code = offset_fetch_response.partitions |> hd |> Map.get(:error_code)
    offset = offset_fetch_response.partitions |> hd |> Map.get(:offset)

    assert error_code == 0
    refute offset == 0
  end

  test "stream starts consuming from last committed offset" do
    random_string = generate_random_string
    KafkaEx.create_worker(:stream_last_committed_offset, uris: uris)
    Enum.each(1..10, fn _ -> KafkaEx.produce(%Proto.Produce.Request{topic: random_string, required_acks: 1, messages: [%Proto.Produce.Message{value: "hey"}]}) end)
    KafkaEx.offset_commit(:stream_last_committed_offset, %Proto.OffsetCommit.Request{topic: random_string, offset: 3})
    log = KafkaEx.stream(random_string, 0, worker_name: :stream_last_committed_offset) |> Enum.take(2)

    refute Enum.empty?(log)

    first_message = log |> hd

    assert first_message.offset == 3
  end

  test "stream does not commit offset with auto_commit is set to false" do
    random_string = generate_random_string
    KafkaEx.create_worker(:stream_no_auto_commit, uris: uris)
    Enum.each(1..10, fn _ -> KafkaEx.produce(%Proto.Produce.Request{topic: random_string, required_acks: 1, messages: [%Proto.Produce.Message{value: "hey"}]}) end)
    KafkaEx.stream(random_string, 0, worker_name: :stream_no_auto_commit, auto_commit: false, offset: 0)

    offset_fetch_response = KafkaEx.offset_fetch(:stream_no_auto_commit, %Proto.OffsetFetch.Request{topic: random_string}) |> hd
    offset_fetch_response.partitions |> hd |> Map.get(:error_code)
    offset_fetch_response_offset = offset_fetch_response.partitions |> hd |> Map.get(:offset)

    assert 0 >= offset_fetch_response_offset
  end
end
