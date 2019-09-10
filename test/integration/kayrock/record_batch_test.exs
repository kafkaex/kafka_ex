defmodule KafkaEx.KayrockRecordBatchTest do
  @moduledoc """
  Tests for producing/fetching messages using the newer RecordBatch format
  """

  use ExUnit.Case

  alias KafkaEx.New.Client

  @moduletag :new_client

  setup do
    {:ok, args} = KafkaEx.build_worker_options([])

    {:ok, pid} = Client.start_link(args, :no_name)

    {:ok, %{client: pid}}
  end

  test "can specify protocol version for fetch - v3", %{client: client} do
    topic = "food"
    msg = TestHelper.generate_random_string()

    {:ok, offset} =
      KafkaEx.produce(
        topic,
        0,
        msg,
        worker_name: client,
        required_acks: 1
      )

    fetch_responses =
      KafkaEx.fetch(topic, 0,
        offset: offset,
        auto_commit: false,
        worker_name: client,
        protocol_version: 3
      )

    [fetch_response | _] = fetch_responses
    [partition_response | _] = fetch_response.partitions
    message = List.last(partition_response.message_set)

    assert message.value == msg
    assert message.offset == offset
  end

  test "fetch empty message set - v3", %{client: client} do
    topic = "food"
    msg = TestHelper.generate_random_string()

    {:ok, offset} =
      KafkaEx.produce(
        topic,
        0,
        msg,
        worker_name: client,
        required_acks: 1
      )

    fetch_responses =
      KafkaEx.fetch(topic, 0,
        offset: offset + 5,
        auto_commit: false,
        worker_name: client,
        protocol_version: 3
      )

    [fetch_response | _] = fetch_responses
    [partition_response | _] = fetch_response.partitions
    assert partition_response.message_set == []
  end

  # v2 is the highest that will accept the MessageSet format
  test "can specify protocol version for produce - v2", %{client: client} do
    topic = "food"
    msg = TestHelper.generate_random_string()

    {:ok, offset} =
      KafkaEx.produce(
        topic,
        0,
        msg,
        worker_name: client,
        required_acks: 1,
        protocol_version: 3
      )

    fetch_responses =
      KafkaEx.fetch(topic, 0,
        offset: offset,
        auto_commit: false,
        worker_name: client,
        protocol_version: 2
      )

    [fetch_response | _] = fetch_responses
    [partition_response | _] = fetch_response.partitions
    message = List.last(partition_response.message_set)

    assert message.value == msg
    assert message.offset == offset
  end

  test "can specify protocol version for fetch - v5", %{client: client} do
    topic = "food"
    msg = TestHelper.generate_random_string()

    {:ok, offset} =
      KafkaEx.produce(
        topic,
        0,
        msg,
        worker_name: client,
        required_acks: 1
      )

    fetch_responses =
      KafkaEx.fetch(topic, 0,
        offset: offset,
        auto_commit: false,
        worker_name: client,
        protocol_version: 5
      )

    [fetch_response | _] = fetch_responses
    [partition_response | _] = fetch_response.partitions
    message = List.last(partition_response.message_set)

    assert message.offset == offset
    assert message.value == msg
  end

  test "fetch empty message set - v5", %{client: client} do
    topic = "food"
    msg = TestHelper.generate_random_string()

    {:ok, offset} =
      KafkaEx.produce(
        topic,
        0,
        msg,
        worker_name: client,
        required_acks: 1
      )

    fetch_responses =
      KafkaEx.fetch(topic, 0,
        offset: offset + 5,
        auto_commit: false,
        worker_name: client,
        protocol_version: 5
      )

    [fetch_response | _] = fetch_responses
    [partition_response | _] = fetch_response.partitions
    assert partition_response.message_set == []
  end

  # v3 is the lowest that requires the RecordBatch format
  test "can specify protocol version for produce - v3", %{client: client} do
    topic = "food"
    msg = TestHelper.generate_random_string()

    {:ok, offset} =
      KafkaEx.produce(
        topic,
        0,
        msg,
        worker_name: client,
        required_acks: 1,
        protocol_version: 3
      )

    fetch_responses =
      KafkaEx.fetch(topic, 0,
        offset: offset,
        auto_commit: false,
        worker_name: client,
        protocol_version: 3
      )

    [fetch_response | _] = fetch_responses
    [partition_response | _] = fetch_response.partitions
    message = List.last(partition_response.message_set)

    assert message.value == msg
    assert message.offset == offset
  end

  test "gzip compression - produce v0, fetch v3", %{client: client} do
    topic = "food"
    msg = TestHelper.generate_random_string()

    {:ok, offset} =
      KafkaEx.produce(
        topic,
        0,
        msg,
        worker_name: client,
        required_acks: 1,
        compression: :gzip,
        protocol_version: 0
      )

    fetch_responses =
      KafkaEx.fetch(topic, 0,
        offset: max(offset - 2, 0),
        auto_commit: false,
        worker_name: client,
        protocol_version: 3
      )

    [fetch_response | _] = fetch_responses
    [partition_response | _] = fetch_response.partitions
    message = List.last(partition_response.message_set)

    assert message.value == msg
    assert message.offset == offset
  end

  test "gzip compression - produce v0, fetch v5", %{client: client} do
    topic = "food"
    msg = TestHelper.generate_random_string()

    {:ok, offset} =
      KafkaEx.produce(
        topic,
        0,
        msg,
        worker_name: client,
        required_acks: 1,
        compression: :gzip,
        protocol_version: 0
      )

    fetch_responses =
      KafkaEx.fetch(topic, 0,
        offset: max(offset - 2, 0),
        auto_commit: false,
        worker_name: client,
        protocol_version: 5
      )

    [fetch_response | _] = fetch_responses
    [partition_response | _] = fetch_response.partitions
    message = List.last(partition_response.message_set)

    assert message.value == msg
    assert message.offset == offset
  end

  test "gzip compression - produce v3, fetch v0", %{client: client} do
    topic = "food"
    msg = TestHelper.generate_random_string()

    {:ok, offset} =
      KafkaEx.produce(
        topic,
        0,
        msg,
        worker_name: client,
        required_acks: 1,
        compression: :gzip,
        protocol_version: 3
      )

    fetch_responses =
      KafkaEx.fetch(topic, 0,
        offset: max(offset - 2, 0),
        auto_commit: false,
        worker_name: client,
        protocol_version: 0
      )

    [fetch_response | _] = fetch_responses
    [partition_response | _] = fetch_response.partitions
    message = List.last(partition_response.message_set)

    assert message.value == msg
    assert message.offset == offset
  end

  test "gzip compression - produce v3, fetch v3", %{client: client} do
    topic = "food"
    msg = TestHelper.generate_random_string()

    {:ok, offset} =
      KafkaEx.produce(
        topic,
        0,
        msg,
        worker_name: client,
        required_acks: 1,
        compression: :gzip,
        protocol_version: 3
      )

    fetch_responses =
      KafkaEx.fetch(topic, 0,
        offset: max(offset - 2, 0),
        auto_commit: false,
        worker_name: client,
        protocol_version: 0
      )

    [fetch_response | _] = fetch_responses
    [partition_response | _] = fetch_response.partitions
    message = List.last(partition_response.message_set)

    assert message.value == msg
    assert message.offset == offset
  end

  test "gzip compression - produce v3, fetch v5", %{client: client} do
    topic = "food"
    msg = TestHelper.generate_random_string()

    {:ok, offset} =
      KafkaEx.produce(
        topic,
        0,
        msg,
        worker_name: client,
        required_acks: 1,
        compression: :gzip,
        protocol_version: 3
      )

    fetch_responses =
      KafkaEx.fetch(topic, 0,
        offset: max(offset - 2, 0),
        auto_commit: false,
        worker_name: client,
        protocol_version: 0
      )

    [fetch_response | _] = fetch_responses
    [partition_response | _] = fetch_response.partitions
    message = List.last(partition_response.message_set)

    assert message.value == msg
    assert message.offset == offset
  end

  test "snappy compression - produce v0, fetch v3", %{client: client} do
    topic = "food"
    msg = TestHelper.generate_random_string()

    {:ok, offset} =
      KafkaEx.produce(
        topic,
        0,
        msg,
        worker_name: client,
        required_acks: 1,
        compression: :snappy,
        protocol_version: 0
      )

    fetch_responses =
      KafkaEx.fetch(topic, 0,
        offset: max(offset - 2, 0),
        auto_commit: false,
        worker_name: client,
        protocol_version: 3
      )

    [fetch_response | _] = fetch_responses
    [partition_response | _] = fetch_response.partitions
    message = List.last(partition_response.message_set)

    assert message.value == msg
    assert message.offset == offset
  end

  test "snappy compression - produce v0, fetch v5", %{client: client} do
    topic = "food"
    msg = TestHelper.generate_random_string()

    {:ok, offset} =
      KafkaEx.produce(
        topic,
        0,
        msg,
        worker_name: client,
        required_acks: 1,
        compression: :snappy,
        protocol_version: 0
      )

    fetch_responses =
      KafkaEx.fetch(topic, 0,
        offset: max(offset - 2, 0),
        auto_commit: false,
        worker_name: client,
        protocol_version: 5
      )

    [fetch_response | _] = fetch_responses
    [partition_response | _] = fetch_response.partitions
    message = List.last(partition_response.message_set)

    assert message.value == msg
    assert message.offset == offset
  end

  test "snappy compression - produce v3, fetch v0", %{client: client} do
    topic = "food"
    msg = TestHelper.generate_random_string()

    {:ok, offset} =
      KafkaEx.produce(
        topic,
        0,
        msg,
        worker_name: client,
        required_acks: 1,
        compression: :snappy,
        protocol_version: 3
      )

    fetch_responses =
      KafkaEx.fetch(topic, 0,
        offset: max(offset - 2, 0),
        auto_commit: false,
        worker_name: client,
        protocol_version: 0
      )

    [fetch_response | _] = fetch_responses
    [partition_response | _] = fetch_response.partitions
    message = List.last(partition_response.message_set)

    assert message.value == msg
    assert message.offset == offset
  end

  test "snappy compression - produce v3, fetch v3", %{client: client} do
    topic = "food"
    msg = TestHelper.generate_random_string()

    {:ok, offset} =
      KafkaEx.produce(
        topic,
        0,
        msg,
        worker_name: client,
        required_acks: 1,
        compression: :snappy,
        protocol_version: 3
      )

    fetch_responses =
      KafkaEx.fetch(topic, 0,
        offset: max(offset - 2, 0),
        auto_commit: false,
        worker_name: client,
        protocol_version: 0
      )

    [fetch_response | _] = fetch_responses
    [partition_response | _] = fetch_response.partitions
    message = List.last(partition_response.message_set)

    assert message.value == msg
    assert message.offset == offset
  end

  test "snappy compression - produce v3, fetch v5", %{client: client} do
    topic = "food"
    msg = TestHelper.generate_random_string()

    {:ok, offset} =
      KafkaEx.produce(
        topic,
        0,
        msg,
        worker_name: client,
        required_acks: 1,
        compression: :snappy,
        protocol_version: 3
      )

    fetch_responses =
      KafkaEx.fetch(topic, 0,
        offset: max(offset - 2, 0),
        auto_commit: false,
        worker_name: client,
        protocol_version: 0
      )

    [fetch_response | _] = fetch_responses
    [partition_response | _] = fetch_response.partitions
    message = List.last(partition_response.message_set)

    assert message.value == msg
    assert message.offset == offset
  end
end
