defmodule KafkaEx.KayrockOffsetTest do
  @moduledoc """
  Tests for offset commit and fetch API versioning
  """

  use ExUnit.Case

  alias KafkaEx.Protocol.OffsetCommit
  alias KafkaEx.Protocol.OffsetFetch

  @moduletag :new_client

  setup do
    {:ok, pid} = KafkaEx.start_link_worker(:no_name, server_impl: KafkaEx.New.Client)

    {:ok, %{client: pid}}
  end

  test "offset commit v0 and fetch v0", %{client: client} do
    topic = "food"
    consumer_group = "commit_v0_fetch_v0"
    msg = KafkaEx.TestHelpers.generate_random_string()

    {:ok, offset} =
      KafkaEx.produce(
        topic,
        0,
        msg,
        worker_name: client,
        required_acks: 1
      )

    [resp] =
      KafkaEx.offset_commit(client, %OffsetCommit.Request{
        consumer_group: consumer_group,
        topic: topic,
        partition: 0,
        offset: offset,
        api_version: 0
      })

    %OffsetCommit.Response{partitions: [%{error_code: :no_error}]} = resp

    [resp] =
      KafkaEx.offset_fetch(client, %OffsetFetch.Request{
        topic: topic,
        consumer_group: consumer_group,
        partition: 0,
        api_version: 0
      })

    %KafkaEx.Protocol.OffsetFetch.Response{
      partitions: [
        %{error_code: :no_error, offset: got_offset, partition: 0}
      ]
    } = resp

    assert got_offset == offset
  end

  test "offset commit v1 and fetch v0", %{client: client} do
    topic = "food"
    consumer_group = "commit_v1_fetch_v0"
    msg = KafkaEx.TestHelpers.generate_random_string()

    {:ok, offset} =
      KafkaEx.produce(
        topic,
        0,
        msg,
        worker_name: client,
        required_acks: 1
      )

    [resp] =
      KafkaEx.offset_commit(client, %OffsetCommit.Request{
        consumer_group: consumer_group,
        topic: topic,
        partition: 0,
        offset: offset,
        api_version: 1
      })

    %OffsetCommit.Response{partitions: [%{error_code: :no_error}]} = resp

    [resp] =
      KafkaEx.offset_fetch(client, %OffsetFetch.Request{
        topic: topic,
        consumer_group: consumer_group,
        partition: 0,
        api_version: 0
      })

    # we get an error code when we commit with v > 0 and fetch with v0
    %KafkaEx.Protocol.OffsetFetch.Response{
      partitions: [
        %{
          error_code: :unknown_topic_or_partition,
          offset: -1,
          partition: 0
        }
      ]
    } = resp
  end

  test "offset commit v1 and fetch v1", %{client: client} do
    topic = "food"
    consumer_group = "commit_v1_fetch_v1"
    msg = KafkaEx.TestHelpers.generate_random_string()

    {:ok, offset} =
      KafkaEx.produce(
        topic,
        0,
        msg,
        worker_name: client,
        required_acks: 1
      )

    [resp] =
      KafkaEx.offset_commit(client, %OffsetCommit.Request{
        consumer_group: consumer_group,
        topic: topic,
        partition: 0,
        offset: offset,
        api_version: 1
      })

    %OffsetCommit.Response{partitions: [%{error_code: :no_error}]} = resp

    [resp] =
      KafkaEx.offset_fetch(client, %OffsetFetch.Request{
        topic: topic,
        consumer_group: consumer_group,
        partition: 0,
        api_version: 1
      })

    %KafkaEx.Protocol.OffsetFetch.Response{
      partitions: [
        %{error_code: :no_error, offset: got_offset, partition: 0}
      ]
    } = resp

    assert got_offset == offset
  end

  test "offset commit v0 and fetch v1", %{client: client} do
    topic = "food"
    consumer_group = "commit_v0_fetch_v1"
    msg = KafkaEx.TestHelpers.generate_random_string()

    {:ok, offset} =
      KafkaEx.produce(
        topic,
        0,
        msg,
        worker_name: client,
        required_acks: 1
      )

    [resp] =
      KafkaEx.offset_commit(client, %OffsetCommit.Request{
        consumer_group: consumer_group,
        topic: topic,
        partition: 0,
        offset: offset,
        api_version: 0
      })

    %OffsetCommit.Response{partitions: [%{error_code: :no_error}]} = resp

    [resp] =
      KafkaEx.offset_fetch(client, %OffsetFetch.Request{
        topic: topic,
        consumer_group: consumer_group,
        partition: 0,
        api_version: 1
      })

    %KafkaEx.Protocol.OffsetFetch.Response{
      partitions: [
        %{error_code: :no_error, offset: got_offset, partition: 0}
      ]
    } = resp

    # can't commit with v0 and fetch with v > 0
    assert got_offset == -1
  end

  test "offset commit v0 and fetch v2", %{client: client} do
    topic = "food"
    consumer_group = "commit_v0_fetch_v2"
    msg = KafkaEx.TestHelpers.generate_random_string()

    {:ok, offset} =
      KafkaEx.produce(
        topic,
        0,
        msg,
        worker_name: client,
        required_acks: 1
      )

    [resp] =
      KafkaEx.offset_commit(client, %OffsetCommit.Request{
        consumer_group: consumer_group,
        topic: topic,
        partition: 0,
        offset: offset,
        api_version: 0
      })

    %OffsetCommit.Response{partitions: [%{error_code: :no_error}]} = resp

    [resp] =
      KafkaEx.offset_fetch(client, %OffsetFetch.Request{
        topic: topic,
        consumer_group: consumer_group,
        partition: 0,
        api_version: 2
      })

    %KafkaEx.Protocol.OffsetFetch.Response{
      partitions: [
        %{error_code: :no_error, offset: got_offset, partition: 0}
      ]
    } = resp

    # can't commit with v0 and fetch with v > 0
    assert got_offset == -1
  end

  test "offset commit v0 and fetch v3", %{client: client} do
    topic = "food"
    consumer_group = "commit_v0_fetch_v3"
    msg = KafkaEx.TestHelpers.generate_random_string()

    {:ok, offset} =
      KafkaEx.produce(
        topic,
        0,
        msg,
        worker_name: client,
        required_acks: 1
      )

    [resp] =
      KafkaEx.offset_commit(client, %OffsetCommit.Request{
        consumer_group: consumer_group,
        topic: topic,
        partition: 0,
        offset: offset,
        api_version: 0
      })

    %OffsetCommit.Response{partitions: [%{error_code: :no_error}]} = resp

    [resp] =
      KafkaEx.offset_fetch(client, %OffsetFetch.Request{
        topic: topic,
        consumer_group: consumer_group,
        partition: 0,
        api_version: 3
      })

    %KafkaEx.Protocol.OffsetFetch.Response{
      partitions: [
        %{error_code: :no_error, offset: got_offset, partition: 0}
      ]
    } = resp

    # can't commit with v0 and fetch with v > 0
    assert got_offset == -1
  end

  test "offset commit v2 and fetch v2", %{client: client} do
    topic = "food"
    consumer_group = "commit_v2_fetch_v2"
    msg = KafkaEx.TestHelpers.generate_random_string()

    {:ok, offset} =
      KafkaEx.produce(
        topic,
        0,
        msg,
        worker_name: client,
        required_acks: 1
      )

    [resp] =
      KafkaEx.offset_commit(client, %OffsetCommit.Request{
        consumer_group: consumer_group,
        topic: topic,
        partition: 0,
        offset: offset,
        api_version: 2
      })

    %OffsetCommit.Response{partitions: [%{error_code: :no_error}]} = resp

    [resp] =
      KafkaEx.offset_fetch(client, %OffsetFetch.Request{
        topic: topic,
        consumer_group: consumer_group,
        partition: 0,
        api_version: 2
      })

    %KafkaEx.Protocol.OffsetFetch.Response{
      partitions: [
        %{error_code: :no_error, offset: got_offset, partition: 0}
      ]
    } = resp

    assert got_offset == offset
  end

  test "offset commit v3 and fetch v3", %{client: client} do
    topic = "food"
    consumer_group = "commit_v3_fetch_v3"
    msg = KafkaEx.TestHelpers.generate_random_string()

    {:ok, offset} =
      KafkaEx.produce(
        topic,
        0,
        msg,
        worker_name: client,
        required_acks: 1
      )

    [resp] =
      KafkaEx.offset_commit(client, %OffsetCommit.Request{
        consumer_group: consumer_group,
        topic: topic,
        partition: 0,
        offset: offset,
        api_version: 3
      })

    %OffsetCommit.Response{partitions: [%{error_code: :no_error}]} = resp

    [resp] =
      KafkaEx.offset_fetch(client, %OffsetFetch.Request{
        topic: topic,
        consumer_group: consumer_group,
        partition: 0,
        api_version: 3
      })

    %KafkaEx.Protocol.OffsetFetch.Response{
      partitions: [
        %{error_code: :no_error, offset: got_offset, partition: 0}
      ]
    } = resp

    assert got_offset == offset
  end
end
