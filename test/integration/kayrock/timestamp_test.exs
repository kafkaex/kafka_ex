defmodule KafkaEx.KayrockTimestampTest do
  @moduledoc """
  Tests for the timestamp functionality in messages
  """

  use ExUnit.Case

  alias KafkaEx.New.Client
  alias KafkaEx.New.NodeSelector
  alias KafkaEx.TimestampNotSupportedError

  require Logger

  @moduletag :new_client

  setup do
    {:ok, args} = KafkaEx.build_worker_options([])

    {:ok, pid} = Client.start_link(args, :no_name)

    {:ok, %{client: pid}}
  end

  defp ensure_append_timestamp_topic(client) do
    topic_name = "test_log_append_timestamp"

    resp =
      Client.send_request(
        client,
        %Kayrock.CreateTopics.V0.Request{
          create_topic_requests: [
            %{
              topic: topic_name,
              num_partitions: 4,
              replication_factor: 1,
              replica_assignment: [],
              config_entries: [
                %{
                  config_name: "message.timestamp.type",
                  config_value: "LogAppendTime"
                }
              ]
            }
          ],
          timeout: 1000
        },
        NodeSelector.controller()
      )

    {:ok,
     %Kayrock.CreateTopics.V0.Response{
       topic_errors: [%{error_code: error_code}]
     }} = resp

    unless error_code in [0, 36] do
      Logger.error("Unable to create topic #{topic_name}: #{inspect(resp)}")
      assert false
    end

    topic_name
  end

  test "fetch timestamp is nil by default on v0 messages", %{client: client} do
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
        api_version: 0
      )

    [fetch_response | _] = fetch_responses
    [partition_response | _] = fetch_response.partitions
    message = List.last(partition_response.message_set)

    assert message.value == msg
    assert message.offset == offset
    assert message.timestamp == nil
  end

  test "fetch timestamp is -1 by default on v3 messages", %{client: client} do
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
    assert message.timestamp == -1
  end

  test "fetch timestamp is -1 by default on v5 messages", %{client: client} do
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

    assert message.value == msg
    assert message.offset == offset
    assert message.timestamp == -1
  end

  test "log with append time - v0", %{client: client} do
    topic = ensure_append_timestamp_topic(client)

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
        api_version: 0
      )

    [fetch_response | _] = fetch_responses
    [partition_response | _] = fetch_response.partitions
    message = List.last(partition_response.message_set)

    assert message.value == msg
    assert message.offset == offset
    assert message.timestamp == nil
  end

  test "log with append time - v3", %{client: client} do
    topic = ensure_append_timestamp_topic(client)

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
    refute is_nil(message.timestamp)
    assert message.timestamp > 0
  end

  test "log with append time - v5", %{client: client} do
    topic = ensure_append_timestamp_topic(client)

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

    assert message.value == msg
    assert message.offset == offset
    refute is_nil(message.timestamp)
    assert message.timestamp > 0
  end

  test "set timestamp with v0 throws an error", %{client: client} do
    topic = "food"

    msg = TestHelper.generate_random_string()

    Process.flag(:trap_exit, true)

    catch_exit do
      KafkaEx.produce(
        topic,
        0,
        msg,
        worker_name: client,
        required_acks: 1,
        timestamp: 12345,
        api_version: 0
      )
    end

    assert_received {:EXIT, ^client, {%TimestampNotSupportedError{}, _}}
  end

  test "set timestamp with v1 throws an error", %{client: client} do
    topic = "food"

    msg = TestHelper.generate_random_string()

    Process.flag(:trap_exit, true)

    catch_exit do
      KafkaEx.produce(
        topic,
        0,
        msg,
        worker_name: client,
        required_acks: 1,
        timestamp: 12345,
        api_version: 1
      )
    end

    assert_received {:EXIT, ^client, {%TimestampNotSupportedError{}, _}}
  end

  test "set timestamp for v3 message, fetch v0", %{client: client} do
    topic = "food"
    msg = TestHelper.generate_random_string()

    {:ok, offset} =
      KafkaEx.produce(
        topic,
        0,
        msg,
        worker_name: client,
        required_acks: 1,
        timestamp: 12345,
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
    assert message.timestamp == nil
  end

  test "set timestamp for v3 message, fetch v3", %{client: client} do
    topic = "food"
    msg = TestHelper.generate_random_string()

    {:ok, offset} =
      KafkaEx.produce(
        topic,
        0,
        msg,
        worker_name: client,
        required_acks: 1,
        timestamp: 12345,
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
    assert message.timestamp == 12345
  end

  test "set timestamp for v3 message, fetch v5", %{client: client} do
    topic = "food"
    msg = TestHelper.generate_random_string()

    {:ok, offset} =
      KafkaEx.produce(
        topic,
        0,
        msg,
        worker_name: client,
        required_acks: 1,
        timestamp: 12345,
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

    assert message.value == msg
    assert message.offset == offset
    assert message.timestamp == 12345
  end
end
