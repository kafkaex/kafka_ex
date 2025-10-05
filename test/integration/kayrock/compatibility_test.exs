defmodule KafkaEx.KayrockCompatibilityTest do
  @moduledoc """
  These are tests using the original KafkaEx API with the kayrock server

  These mostly come from the original integration_test.exs file
  """
  use ExUnit.Case
  import KafkaEx.TestHelpers
  import KafkaEx.IntegrationHelpers

  @moduletag :new_client

  alias KafkaEx.New.Client
  alias KafkaEx.Protocol, as: Proto
  alias KafkaEx.Protocol.Metadata.Response, as: MetadataResponse
  alias KafkaEx.Protocol.Metadata.Broker
  alias KafkaEx.Protocol.Metadata.TopicMetadata
  alias KafkaEx.Protocol.Offset.Response, as: OffsetResponse

  alias KafkaEx.New.Structs.ClusterMetadata
  alias KafkaEx.New.KafkaExAPI

  setup do
    {:ok, pid} = KafkaEx.start_link_worker(:no_name, server_impl: KafkaEx.New.Client)

    {:ok, %{client: pid}}
  end

  test "Creates a worker even when the one of the provided brokers is not available" do
    uris = Application.get_env(:kafka_ex, :brokers) ++ [{"bad_host", 9000}]
    {:ok, args} = KafkaEx.build_worker_options(uris: uris)

    {:ok, pid} = Client.start_link(args, :no_name)

    assert Process.alive?(pid)
  end

  describe "describe_groups/1" do
    setup do
      consumer_group = generate_random_string()
      topic = "new_client_implementation"

      {:ok, %{consumer_group: consumer_group, topic: topic}}
    end

    test "with new client - returns group metadata", %{client: client, consumer_group: consumer_group, topic: topic} do
      join_to_group(client, topic, consumer_group)

      {:ok, group_metadata} = KafkaExAPI.describe_group(client, consumer_group)

      assert group_metadata.group_id == consumer_group
      assert group_metadata.protocol_type == "consumer"
      assert group_metadata.protocol == ""
      assert length(group_metadata.members) == 1
    end

    test "with old client - returns group metadata", %{client: client, consumer_group: consumer_group, topic: topic} do
      join_to_group(client, topic, consumer_group)

      {:ok, group_metadata} = KafkaEx.describe_group(consumer_group, worker_name: client)

      assert group_metadata.group_id == consumer_group
      assert group_metadata.protocol_type == "consumer"
      assert group_metadata.protocol == ""
      assert length(group_metadata.members) == 1
    end
  end

  test "worker updates metadata after specified interval" do
    {:ok, args} = KafkaEx.build_worker_options(metadata_update_interval: 100)
    {:ok, pid} = Client.start_link(args, :no_name)
    previous_corr_id = KafkaExAPI.correlation_id(pid)

    :timer.sleep(105)
    curr_corr_id = KafkaExAPI.correlation_id(pid)
    refute curr_corr_id == previous_corr_id
  end

  test "get metadata", %{client: client} do
    metadata = KafkaEx.metadata(worker_name: client, topic: "test0p8p0")

    %MetadataResponse{topic_metadatas: topic_metadatas, brokers: brokers} = metadata

    assert is_list(topic_metadatas)
    [topic_metadata | _] = topic_metadatas
    assert %TopicMetadata{} = topic_metadata
    refute topic_metadatas == []
    assert is_list(brokers)
    [broker | _] = brokers
    assert %Broker{} = broker
  end

  test "list offsets", %{client: client} do
    topic = "test0p8p0"

    resp = KafkaEx.offset(topic, 0, :earliest, client)

    [%OffsetResponse{topic: ^topic, partition_offsets: [partition_offsets]}] = resp

    %{error_code: :no_error, offset: [offset], partition: 0} = partition_offsets
    assert offset >= 0
  end

  test "produce/4 without an acq required returns :ok", %{client: client} do
    assert KafkaEx.produce("food", 0, "hey", worker_name: client, required_acks: 0) == :ok
  end

  test "produce/4 with ack required returns an ack", %{client: client} do
    {:ok, offset} = KafkaEx.produce("food", 0, "hey", worker_name: client, required_acks: 1)

    assert is_integer(offset)
    refute offset == nil
  end

  test "produce without an acq required returns :ok", %{client: client} do
    assert KafkaEx.produce(
             %Proto.Produce.Request{
               topic: "food",
               partition: 0,
               required_acks: 0,
               messages: [%Proto.Produce.Message{value: "hey"}]
             },
             worker_name: client
           ) == :ok
  end

  test "produce with ack required returns an ack", %{client: client} do
    {:ok, offset} =
      KafkaEx.produce(
        %Proto.Produce.Request{
          topic: "food",
          partition: 0,
          required_acks: 1,
          messages: [%Proto.Produce.Message{value: "hey"}]
        },
        worker_name: client
      )

    refute offset == nil
  end

  test "produce creates log for a non-existing topic", %{client: client} do
    random_string = KafkaEx.TestHelpers.generate_random_string()

    KafkaEx.produce(
      %Proto.Produce.Request{
        topic: random_string,
        partition: 0,
        required_acks: 1,
        messages: [%Proto.Produce.Message{value: "hey"}]
      },
      worker_name: client
    )

    {:ok, cluster_metadata} = KafkaExAPI.cluster_metadata(client)

    assert random_string in ClusterMetadata.known_topics(cluster_metadata)
  end

  test "fetch does not blow up with incomplete bytes", %{client: client} do
    KafkaEx.fetch("food", 0,
      offset: 0,
      max_bytes: 100,
      auto_commit: false,
      worker_name: client
    )
  end

  test "fetch returns ':topic_not_found' for non-existing topic", %{
    client: client
  } do
    random_string = KafkaEx.TestHelpers.generate_random_string()

    assert KafkaEx.fetch(random_string, 0, offset: 0, worker_name: client) ==
             :topic_not_found
  end

  test "fetch works", %{client: client} do
    topic_name = KafkaEx.TestHelpers.generate_random_string()

    {:ok, offset} =
      KafkaEx.produce(
        %Proto.Produce.Request{
          topic: topic_name,
          partition: 0,
          required_acks: 1,
          messages: [%Proto.Produce.Message{value: "hey foo"}]
        },
        worker_name: client
      )

    fetch_responses =
      KafkaEx.fetch(topic_name, 0,
        offset: 0,
        auto_commit: false,
        worker_name: client
      )

    [fetch_response | _] = fetch_responses

    message = fetch_response.partitions |> hd |> Map.get(:message_set) |> hd

    assert message.partition == 0
    assert message.topic == topic_name
    assert message.value == "hey foo"
    assert message.offset == offset
  end

  test "fetch with empty topic", %{client: client} do
    random_string = KafkaEx.TestHelpers.generate_random_string()

    response =
      KafkaEx.fetch(random_string, 0,
        offset: 0,
        auto_commit: false,
        worker_name: client
      )

    assert response == :topic_not_found
  end

  test "fetch nonexistent offset", %{client: client} do
    random_string = KafkaEx.TestHelpers.generate_random_string()

    {:ok, offset} =
      KafkaEx.produce(
        %Proto.Produce.Request{
          topic: random_string,
          partition: 0,
          required_acks: 1,
          messages: [%Proto.Produce.Message{value: "hey foo"}]
        },
        worker_name: client
      )

    fetch_responses =
      KafkaEx.fetch(random_string, 0,
        offset: offset + 5,
        auto_commit: false,
        worker_name: client
      )

    [fetch_response | _] = fetch_responses
    [partition | _] = fetch_response.partitions
    assert partition.error_code == :offset_out_of_range
  end

  test "fetch nonexistent partition", %{client: client} do
    random_string = KafkaEx.TestHelpers.generate_random_string()

    {:ok, offset} =
      KafkaEx.produce(
        %Proto.Produce.Request{
          topic: random_string,
          partition: 0,
          required_acks: 1,
          messages: [%Proto.Produce.Message{value: "hey foo"}]
        },
        worker_name: client
      )

    fetch_responses =
      KafkaEx.fetch(random_string, 99,
        offset: offset + 5,
        auto_commit: false,
        worker_name: client
      )

    assert fetch_responses == :topic_not_found
  end

  test "earliest_offset retrieves offset of 0", %{client: client} do
    random_string = KafkaEx.TestHelpers.generate_random_string()

    KafkaEx.produce(
      %Proto.Produce.Request{
        topic: random_string,
        partition: 0,
        required_acks: 1,
        messages: [%Proto.Produce.Message{value: "hey"}]
      },
      worker_name: client
    )

    offset_response = KafkaEx.earliest_offset(random_string, 0, client) |> hd

    offset = offset_response.partition_offsets |> hd |> Map.get(:offset) |> hd

    assert offset == 0
  end

  test "latest_offset retrieves offset of 0 for non-existing topic", %{client: client} do
    random_string = KafkaEx.TestHelpers.generate_random_string()

    {:ok, produce_offset} =
      KafkaEx.produce(
        %Proto.Produce.Request{
          topic: random_string,
          partition: 0,
          required_acks: 1,
          messages: [%Proto.Produce.Message{value: "hey"}]
        },
        worker_name: client
      )

    :timer.sleep(300)
    offset_response = KafkaEx.latest_offset(random_string, 0, client) |> hd
    offset = offset_response.partition_offsets |> hd |> Map.get(:offset) |> hd

    assert offset == produce_offset + 1
  end

  test "latest_offset retrieves a non-zero offset for a topic published to", %{
    client: client
  } do
    random_string = KafkaEx.TestHelpers.generate_random_string()

    KafkaEx.produce(
      %Proto.Produce.Request{
        topic: random_string,
        partition: 0,
        required_acks: 1,
        messages: [%Proto.Produce.Message{value: "foo"}]
      },
      worker_name: client
    )

    [offset_response] =
      KafkaEx.TestHelpers.wait_for_any(fn ->
        KafkaEx.latest_offset(random_string, 0, client)
      end)

    offset = offset_response.partition_offsets |> hd |> Map.get(:offset) |> hd

    assert offset != 0
  end

  # compression
  test "compresses / decompresses using gzip", %{client: client} do
    random_string = KafkaEx.TestHelpers.generate_random_string()

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

    {:ok, offset} = KafkaEx.produce(produce_request, worker_name: client)

    fetch_response =
      KafkaEx.fetch(random_string, 0,
        offset: 0,
        auto_commit: false,
        worker_name: client
      )
      |> hd

    [got_message1, got_message2] = fetch_response.partitions |> hd |> Map.get(:message_set)

    assert got_message1.key == message1.key
    assert got_message1.value == message1.value
    assert got_message1.offset == offset
    assert got_message2.key == message2.key
    assert got_message2.value == message2.value
    assert got_message2.offset == offset + 1
  end

  test "compresses / decompresses using snappy", %{client: client} do
    random_string = KafkaEx.TestHelpers.generate_random_string()

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

    {:ok, offset} = KafkaEx.produce(produce_request, worker_name: client)

    fetch_response =
      KafkaEx.fetch(random_string, 0,
        offset: 0,
        auto_commit: false,
        worker_name: client
      )
      |> hd

    [got_message1, got_message2] = fetch_response.partitions |> hd |> Map.get(:message_set)

    assert got_message1.key == message1.key
    assert got_message1.value == message1.value
    assert got_message1.offset == offset
    assert got_message2.key == message2.key
    assert got_message2.value == message2.value
    assert got_message2.offset == offset + 1
  end

  # larger messages
  test "publish/fetch handles a 10kb message", %{client: client} do
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

    {:ok, offset} = KafkaEx.produce(produce_request, worker_name: client)

    fetch_response = KafkaEx.fetch(topic, 0, offset: offset, worker_name: client) |> hd

    [got_message] = fetch_response.partitions |> hd |> Map.get(:message_set)

    assert nil == got_message.key
    assert message_value == got_message.value
  end

  test "publish/fetch handles a 10kb message snappy compressed", %{
    client: client
  } do
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

    {:ok, offset} = KafkaEx.produce(produce_request, worker_name: client)

    fetch_response = KafkaEx.fetch(topic, 0, offset: offset, worker_name: client) |> hd

    [got_message] = fetch_response.partitions |> hd |> Map.get(:message_set)

    assert nil == got_message.key
    assert message_value == got_message.value
  end

  # stream
  test "streams kafka logs", %{client: client} do
    topic_name = KafkaEx.TestHelpers.generate_random_string()

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
      worker_name: client
    )

    stream =
      KafkaEx.stream(
        topic_name,
        0,
        worker_name: client,
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

  test "stream with small max_bytes makes multiple requests if necessary", %{
    client: client
  } do
    topic_name = KafkaEx.TestHelpers.generate_random_string()

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
      worker_name: client
    )

    stream =
      KafkaEx.stream(
        topic_name,
        0,
        worker_name: client,
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

  test "stream blocks until new messages are available", %{client: client} do
    topic_name = KafkaEx.TestHelpers.generate_random_string()

    KafkaEx.produce(
      %Proto.Produce.Request{
        topic: topic_name,
        partition: 0,
        required_acks: 1,
        messages: [
          %Proto.Produce.Message{value: "message 1"}
        ]
      },
      worker_name: client
    )

    stream =
      KafkaEx.stream(
        topic_name,
        0,
        worker_name: client,
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
      worker_name: client
    )

    logs = Task.await(task)

    assert 4 == length(logs)
    [m1, m2, m3, m4] = logs
    assert m1.value == "message 1"
    assert m2.value == "message 2"
    assert m3.value == "message 3"
    assert m4.value == "message 4"
  end

  test "stream is non-blocking with no_wait_at_logend", %{client: client} do
    topic_name = KafkaEx.TestHelpers.generate_random_string()

    KafkaEx.produce(
      %Proto.Produce.Request{
        topic: topic_name,
        partition: 0,
        required_acks: 1,
        messages: [
          %Proto.Produce.Message{value: "message 1"}
        ]
      },
      worker_name: client
    )

    stream =
      KafkaEx.stream(
        topic_name,
        0,
        worker_name: client,
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

  test "doesn't error when re-creating an existing stream", %{client: client} do
    random_string = KafkaEx.TestHelpers.generate_random_string()
    KafkaEx.stream(random_string, 0, offset: 0, worker_name: client)
    KafkaEx.stream(random_string, 0, offset: 0, worker_name: client)
  end

  test "produce with the default partitioner works", %{client: client} do
    topic = KafkaEx.TestHelpers.generate_random_string()
    :ok = KafkaEx.produce(topic, nil, "hello", worker_name: client)
  end

  describe "offset_fetch compatibility" do
    test "fetch committed offset via legacy API", %{client: client} do
      topic_name = KafkaEx.TestHelpers.generate_random_string()
      consumer_group = KafkaEx.TestHelpers.generate_random_string()
      create_topic(client, topic_name, partitions: 1)

      # Commit via new API
      partitions = [%{partition_num: 0, offset: 123}]
      {:ok, commit_result} = KafkaExAPI.commit_offset(client, consumer_group, topic_name, partitions)

      # Verify commit succeeded
      assert [commit_response] = commit_result
      assert commit_response.topic == topic_name
      assert [commit_partition] = commit_response.partition_offsets
      assert commit_partition.error_code == :no_error

      # Small delay to ensure Kafka has processed the commit
      Process.sleep(100)

      # Fetch via legacy API (use v1 to match new API's Kafka-based storage)
      request = %KafkaEx.Protocol.OffsetFetch.Request{
        topic: topic_name,
        partition: 0,
        consumer_group: consumer_group,
        api_version: 1
      }

      [response] = KafkaEx.offset_fetch(client, request)

      assert response.topic == topic_name
      assert [partition_offset] = response.partitions
      assert partition_offset.partition == 0
      assert partition_offset.offset == 123
    end

    test "fetch via new API what was committed via legacy API", %{client: client} do
      topic_name = KafkaEx.TestHelpers.generate_random_string()
      consumer_group = KafkaEx.TestHelpers.generate_random_string()
      create_topic(client, topic_name, partitions: 1)

      # Commit via legacy API (use v1 for standalone commits)
      request = %KafkaEx.Protocol.OffsetCommit.Request{
        topic: topic_name,
        partition: 0,
        offset: 456,
        consumer_group: consumer_group,
        api_version: 1
      }

      commit_response = KafkaEx.offset_commit(client, request)

      # Verify commit succeeded
      assert [commit_result] = commit_response
      assert commit_result.topic == topic_name

      # Small delay to ensure Kafka has processed the commit
      Process.sleep(100)

      # Fetch via new API
      partitions = [%{partition_num: 0}]
      {:ok, [offset]} = KafkaExAPI.fetch_committed_offset(client, consumer_group, topic_name, partitions)

      assert offset.topic == topic_name
      assert [partition_offset] = offset.partition_offsets
      assert partition_offset.offset == 456
    end
  end

  describe "offset_commit compatibility" do
    test "commit offset via legacy API", %{client: client} do
      topic_name = KafkaEx.TestHelpers.generate_random_string()
      consumer_group = KafkaEx.TestHelpers.generate_random_string()
      create_topic(client, topic_name, partitions: 1)

      # Use v1 for standalone offset commits (no consumer group session)
      request = %KafkaEx.Protocol.OffsetCommit.Request{
        topic: topic_name,
        partition: 0,
        offset: 789,
        consumer_group: consumer_group,
        api_version: 1
      }

      response = KafkaEx.offset_commit(client, request)

      assert [result] = response
      assert result.topic == topic_name
      assert [partition_result] = result.partitions
      assert partition_result.partition == 0
      assert partition_result.error_code == :no_error
    end

    test "commit via new API and verify via legacy API", %{client: client} do
      topic_name = KafkaEx.TestHelpers.generate_random_string()
      consumer_group = KafkaEx.TestHelpers.generate_random_string()
      create_topic(client, topic_name, partitions: 1)

      # Commit via new API
      partitions = [%{partition_num: 0, offset: 999}]
      {:ok, commit_result} = KafkaExAPI.commit_offset(client, consumer_group, topic_name, partitions)

      # Verify commit succeeded
      assert [commit_response] = commit_result
      assert commit_response.topic == topic_name

      # Small delay to ensure Kafka has processed the commit
      Process.sleep(100)

      # Verify via legacy API (use v1 to match new API's Kafka-based storage)
      fetch_request = %KafkaEx.Protocol.OffsetFetch.Request{
        topic: topic_name,
        partition: 0,
        consumer_group: consumer_group,
        api_version: 1
      }

      [response] = KafkaEx.offset_fetch(client, fetch_request)

      assert [partition_offset] = response.partitions
      assert partition_offset.offset == 999
    end

    test "round trip commit-then-fetch with mixed APIs", %{client: client} do
      topic_name = KafkaEx.TestHelpers.generate_random_string()
      consumer_group = KafkaEx.TestHelpers.generate_random_string()
      create_topic(client, topic_name, partitions: 1)

      # Commit via legacy API (use v1 for standalone commits)
      commit_request = %KafkaEx.Protocol.OffsetCommit.Request{
        topic: topic_name,
        partition: 0,
        offset: 555,
        consumer_group: consumer_group,
        api_version: 1
      }

      commit_response = KafkaEx.offset_commit(client, commit_request)

      # Verify commit succeeded
      assert [commit_result] = commit_response
      assert commit_result.topic == topic_name

      # Small delay to ensure Kafka has processed the commit
      Process.sleep(100)

      # Fetch via new API
      partitions_fetch = [%{partition_num: 0}]
      {:ok, [offset]} = KafkaExAPI.fetch_committed_offset(client, consumer_group, topic_name, partitions_fetch)

      assert [partition_offset] = offset.partition_offsets
      assert partition_offset.offset == 555

      # Commit via new API
      partitions_commit = [%{partition_num: 0, offset: 777}]
      {:ok, commit_result2} = KafkaExAPI.commit_offset(client, consumer_group, topic_name, partitions_commit)

      # Verify commit succeeded
      assert [commit_response2] = commit_result2
      assert commit_response2.topic == topic_name

      # Small delay to ensure Kafka has processed the commit
      Process.sleep(100)

      # Fetch via legacy API (use v1 to match new API's Kafka-based storage)
      fetch_request = %KafkaEx.Protocol.OffsetFetch.Request{
        topic: topic_name,
        partition: 0,
        consumer_group: consumer_group,
        api_version: 1
      }

      [response] = KafkaEx.offset_fetch(client, fetch_request)

      assert [final_partition_offset] = response.partitions
      assert final_partition_offset.offset == 777
    end
  end

  # -----------------------------------------------------------------------------
  describe "heartbeat compatibility" do
    test "legacy API can heartbeat to group created with new API", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)

      # Join group to get member_id and generation_id
      {member_id, generation_id} = join_to_group(client, topic_name, consumer_group)

      # Heartbeat via new API
      {:ok, result} = KafkaExAPI.heartbeat(client, consumer_group, member_id, generation_id)
      assert result == :no_error

      # Heartbeat via legacy API
      legacy_request = %KafkaEx.Protocol.Heartbeat.Request{
        group_name: consumer_group,
        member_id: member_id,
        generation_id: generation_id
      }

      legacy_response = KafkaEx.heartbeat(legacy_request, worker_name: client)
      assert legacy_response.error_code == :no_error
    end

    test "new API can heartbeat to group created with legacy API", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)

      # Join group to get member_id and generation_id
      {member_id, generation_id} = join_to_group(client, topic_name, consumer_group)

      # Heartbeat via legacy API first
      legacy_request = %KafkaEx.Protocol.Heartbeat.Request{
        group_name: consumer_group,
        member_id: member_id,
        generation_id: generation_id
      }

      legacy_response = KafkaEx.heartbeat(legacy_request, worker_name: client)
      assert legacy_response.error_code == :no_error

      # Heartbeat via new API
      {:ok, result} = KafkaExAPI.heartbeat(client, consumer_group, member_id, generation_id)
      assert result == :no_error
    end

    test "both APIs handle unknown_member_id error identically", %{client: client} do
      consumer_group = generate_random_string()

      # New API
      {:error, error_new} = KafkaExAPI.heartbeat(client, consumer_group, "invalid-member", 0)
      assert error_new == :unknown_member_id

      # Legacy API
      legacy_request = %KafkaEx.Protocol.Heartbeat.Request{
        group_name: consumer_group,
        member_id: "invalid-member",
        generation_id: 0
      }

      legacy_response = KafkaEx.heartbeat(legacy_request, worker_name: client)
      assert legacy_response.error_code == :unknown_member_id
    end

    test "both APIs handle illegal_generation error identically", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)

      # Join group
      {member_id, _generation_id} = join_to_group(client, topic_name, consumer_group)

      # New API
      {:error, error_new} = KafkaExAPI.heartbeat(client, consumer_group, member_id, 999_999)
      assert error_new == :illegal_generation

      # Legacy API
      legacy_request = %KafkaEx.Protocol.Heartbeat.Request{
        group_name: consumer_group,
        member_id: member_id,
        generation_id: 999_999
      }

      legacy_response = KafkaEx.heartbeat(legacy_request, worker_name: client)
      assert legacy_response.error_code == :illegal_generation
    end

    test "multiple heartbeats across APIs maintain group membership", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)

      # Join group
      {member_id, generation_id} = join_to_group(client, topic_name, consumer_group)

      # Alternate between APIs
      {:ok, _} = KafkaExAPI.heartbeat(client, consumer_group, member_id, generation_id)
      Process.sleep(50)

      legacy_request = %KafkaEx.Protocol.Heartbeat.Request{
        group_name: consumer_group,
        member_id: member_id,
        generation_id: generation_id
      }

      legacy_response = KafkaEx.heartbeat(legacy_request, worker_name: client)
      assert legacy_response.error_code == :no_error
      Process.sleep(50)

      {:ok, _} = KafkaExAPI.heartbeat(client, consumer_group, member_id, generation_id)

      # Verify still in group
      {:ok, group} = KafkaExAPI.describe_group(client, consumer_group)
      assert group.group_id == consumer_group
      assert length(group.members) == 1
    end

    test "heartbeat v1 with new API vs v0 with legacy API", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)

      # Join group
      {member_id, generation_id} = join_to_group(client, topic_name, consumer_group)

      # New API with v1 (returns Heartbeat struct)
      {:ok, result_v1} = KafkaExAPI.heartbeat(client, consumer_group, member_id, generation_id, api_version: 1)
      assert %KafkaEx.New.Structs.Heartbeat{throttle_time_ms: _} = result_v1

      # Legacy API uses v0 (returns error_code only)
      legacy_request = %KafkaEx.Protocol.Heartbeat.Request{
        group_name: consumer_group,
        member_id: member_id,
        generation_id: generation_id
      }

      legacy_response = KafkaEx.heartbeat(legacy_request, worker_name: client)
      assert legacy_response.error_code == :no_error

      # New API with v0 (matches legacy behavior)
      {:ok, result_v0} = KafkaExAPI.heartbeat(client, consumer_group, member_id, generation_id, api_version: 0)
      assert result_v0 == :no_error
    end
  end

  # -----------------------------------------------------------------------------
end
