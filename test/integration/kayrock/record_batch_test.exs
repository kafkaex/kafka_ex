defmodule KafkaEx.KayrockRecordBatchTest do
  @moduledoc """
  Tests for producing/fetching messages using the newer RecordBatch format
  """

  use ExUnit.Case

  @moduletag :new_client

  setup do
    {:ok, pid} =
      KafkaEx.start_link_worker(:no_name, server_impl: KafkaEx.New.Client)

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
        api_version: 3
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
        api_version: 3
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
        api_version: 3
      )

    fetch_responses =
      KafkaEx.fetch(topic, 0,
        offset: offset,
        auto_commit: false,
        worker_name: client,
        api_version: 2
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
        api_version: 5
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
        api_version: 5
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
        api_version: 3
      )

    fetch_responses =
      KafkaEx.fetch(topic, 0,
        offset: offset,
        auto_commit: false,
        worker_name: client,
        api_version: 3
      )

    [fetch_response | _] = fetch_responses
    [partition_response | _] = fetch_response.partitions
    message = List.last(partition_response.message_set)

    assert message.value == msg
    assert message.offset == offset
  end

  test "can specify protocol version for produce - v5 with headers", %{
    client: client
  } do
    topic = "food"

    record_headers = [
      {"theHeaderKeyI", "theHeaderValueI"},
      {"theHeaderKeyII", "theHeaderValueII"}
    ]

    msg = %KafkaEx.Protocol.Produce.Message{
      key: "theKey",
      value: "theValue",
      headers: record_headers,
      timestamp: nil
    }

    request = %KafkaEx.Protocol.Produce.Request{
      topic: topic,
      partition: 0,
      required_acks: 1,
      timeout: 100,
      compression: :none,
      messages: [msg],
      api_version: 3
    }

    {:ok, offset} =
      KafkaEx.produce(
        request,
        worker_name: client,
        required_acks: 1,
        api_version: 3
      )

    fetch_responses =
      KafkaEx.fetch(topic, 0,
        offset: offset,
        auto_commit: false,
        worker_name: client,
        api_version: 5
      )

    [fetch_response | _] = fetch_responses
    [partition_response | _] = fetch_response.partitions
    message = List.last(partition_response.message_set)

    assert message.key == "theKey"
    assert message.value == "theValue"
    assert message.headers == record_headers
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
        api_version: 0
      )

    fetch_responses =
      KafkaEx.fetch(topic, 0,
        offset: offset,
        auto_commit: false,
        worker_name: client,
        api_version: 3
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
        api_version: 0
      )

    fetch_responses =
      KafkaEx.fetch(topic, 0,
        offset: offset,
        auto_commit: false,
        worker_name: client,
        api_version: 5
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
        api_version: 3
      )

    fetch_responses =
      KafkaEx.fetch(topic, 0,
        offset: offset,
        auto_commit: false,
        worker_name: client,
        api_version: 0
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
        api_version: 3
      )

    fetch_responses =
      KafkaEx.fetch(topic, 0,
        offset: offset,
        auto_commit: false,
        worker_name: client,
        api_version: 0
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
        api_version: 3
      )

    fetch_responses =
      KafkaEx.fetch(topic, 0,
        offset: offset,
        auto_commit: false,
        worker_name: client,
        api_version: 0
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
        api_version: 0
      )

    fetch_responses =
      KafkaEx.fetch(topic, 0,
        offset: offset,
        auto_commit: false,
        worker_name: client,
        api_version: 3
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
        api_version: 0
      )

    fetch_responses =
      KafkaEx.fetch(topic, 0,
        offset: offset,
        auto_commit: false,
        worker_name: client,
        api_version: 5
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
        api_version: 3
      )

    fetch_responses =
      KafkaEx.fetch(topic, 0,
        offset: offset,
        auto_commit: false,
        worker_name: client,
        api_version: 0
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
        api_version: 3
      )

    fetch_responses =
      KafkaEx.fetch(topic, 0,
        offset: offset,
        auto_commit: false,
        worker_name: client,
        api_version: 0
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
        api_version: 3
      )

    fetch_responses =
      KafkaEx.fetch(topic, 0,
        offset: offset,
        auto_commit: false,
        worker_name: client,
        api_version: 0
      )

    [fetch_response | _] = fetch_responses
    [partition_response | _] = fetch_response.partitions
    message = List.last(partition_response.message_set)

    assert message.value == msg
    assert message.offset == offset
  end
end
