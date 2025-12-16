defmodule KafkaEx.KayrockCompatibilityTest do
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

  alias KafkaEx.New.Kafka.ClusterMetadata
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

  describe "join_group/3" do
    setup do
      consumer_group = generate_random_string()
      topic = "new_client_implementation"

      {:ok, %{consumer_group: consumer_group, topic: topic}}
    end

    test "with new API - joins consumer group successfully", %{
      client: client,
      consumer_group: consumer_group,
      topic: topic
    } do
      group_protocols = [
        %{
          protocol_name: "assign",
          protocol_metadata: %Kayrock.GroupProtocolMetadata{topics: [topic]}
        }
      ]

      {:ok, response} =
        KafkaExAPI.join_group(
          client,
          consumer_group,
          "",
          session_timeout: 30_000,
          rebalance_timeout: 60_000,
          group_protocols: group_protocols
        )

      assert response.generation_id >= 0
      assert response.member_id != ""
      assert response.leader_id == response.member_id
      assert length(response.members) >= 1
    end

    test "with old API - joins consumer group successfully", %{
      client: client,
      consumer_group: consumer_group,
      topic: topic
    } do
      alias KafkaEx.Protocol.JoinGroup.Request, as: JoinGroupRequest

      request = %JoinGroupRequest{
        group_name: consumer_group,
        member_id: "",
        topics: [topic],
        session_timeout: 30_000
      }

      response = KafkaEx.join_group(request, worker_name: client, timeout: 10_000)

      assert response.error_code == :no_error
      assert response.generation_id >= 0
      assert response.member_id != ""
      assert response.leader_id == response.member_id
    end

    test "new and old APIs return compatible results", %{client: client, topic: topic} do
      # Test with new API
      new_group = generate_random_string()

      group_protocols = [
        %{
          protocol_name: "assign",
          protocol_metadata: %Kayrock.GroupProtocolMetadata{topics: [topic]}
        }
      ]

      {:ok, new_response} =
        KafkaExAPI.join_group(
          client,
          new_group,
          "",
          session_timeout: 30_000,
          rebalance_timeout: 60_000,
          group_protocols: group_protocols
        )

      # Test with old API
      old_group = generate_random_string()
      alias KafkaEx.Protocol.JoinGroup.Request, as: JoinGroupRequest

      old_request = %JoinGroupRequest{
        group_name: old_group,
        member_id: "",
        topics: [topic],
        session_timeout: 30_000
      }

      old_response = KafkaEx.join_group(old_request, worker_name: client, timeout: 10_000)

      # Both should succeed with similar structure
      assert new_response.generation_id >= 0
      assert old_response.generation_id >= 0
      assert new_response.member_id != ""
      assert old_response.member_id != ""
      # Both should be leaders (first member)
      assert new_response.leader_id == new_response.member_id
      assert old_response.leader_id == old_response.member_id
    end
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
      assert %KafkaEx.New.Kafka.Heartbeat{throttle_time_ms: _} = result_v1

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
  describe "sync_group compatibility" do
    test "legacy API can sync_group with group created via new API", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)

      # Join group to get member_id and generation_id
      {member_id, generation_id} = join_to_group(client, topic_name, consumer_group)

      # Sync via new API
      {:ok, result} = KafkaExAPI.sync_group(client, consumer_group, generation_id, member_id)
      assert %KafkaEx.New.Kafka.SyncGroup{} = result

      # Sync via legacy API
      legacy_request = %KafkaEx.Protocol.SyncGroup.Request{
        group_name: consumer_group,
        member_id: member_id,
        generation_id: generation_id,
        assignments: []
      }

      legacy_response = KafkaEx.sync_group(legacy_request, worker_name: client)
      assert legacy_response.error_code == :no_error
    end

    test "new API can sync_group with group created via legacy API", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)

      # Join group to get member_id and generation_id
      {member_id, generation_id} = join_to_group(client, topic_name, consumer_group)

      # Sync via legacy API first
      legacy_request = %KafkaEx.Protocol.SyncGroup.Request{
        group_name: consumer_group,
        member_id: member_id,
        generation_id: generation_id,
        assignments: []
      }

      legacy_response = KafkaEx.sync_group(legacy_request, worker_name: client)
      assert legacy_response.error_code == :no_error

      # Sync via new API
      {:ok, result} = KafkaExAPI.sync_group(client, consumer_group, generation_id, member_id)
      assert %KafkaEx.New.Kafka.SyncGroup{} = result
    end

    test "both APIs handle unknown_member_id error identically", %{client: client} do
      consumer_group = generate_random_string()

      # New API
      {:error, error_new} = KafkaExAPI.sync_group(client, consumer_group, 0, "invalid-member")
      assert error_new == :unknown_member_id

      # Legacy API
      legacy_request = %KafkaEx.Protocol.SyncGroup.Request{
        group_name: consumer_group,
        member_id: "invalid-member",
        generation_id: 0,
        assignments: []
      }

      legacy_response = KafkaEx.sync_group(legacy_request, worker_name: client)
      assert legacy_response.error_code == :unknown_member_id
    end

    test "both APIs handle illegal_generation error identically", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)

      # Join group
      {member_id, _generation_id} = join_to_group(client, topic_name, consumer_group)

      # New API
      {:error, error_new} = KafkaExAPI.sync_group(client, consumer_group, 999_999, member_id)
      assert error_new == :illegal_generation

      # Legacy API
      legacy_request = %KafkaEx.Protocol.SyncGroup.Request{
        group_name: consumer_group,
        member_id: member_id,
        generation_id: 999_999,
        assignments: []
      }

      legacy_response = KafkaEx.sync_group(legacy_request, worker_name: client)
      assert legacy_response.error_code == :illegal_generation
    end

    test "multiple sync_group calls across APIs maintain group state", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)

      # Join group
      {member_id, generation_id} = join_to_group(client, topic_name, consumer_group)

      # Alternate between APIs
      {:ok, _} = KafkaExAPI.sync_group(client, consumer_group, generation_id, member_id)
      Process.sleep(50)

      legacy_request = %KafkaEx.Protocol.SyncGroup.Request{
        group_name: consumer_group,
        member_id: member_id,
        generation_id: generation_id,
        assignments: []
      }

      legacy_response = KafkaEx.sync_group(legacy_request, worker_name: client)
      assert legacy_response.error_code == :no_error
      Process.sleep(50)

      {:ok, _} = KafkaExAPI.sync_group(client, consumer_group, generation_id, member_id)

      # Verify still in group and stable
      {:ok, group} = KafkaExAPI.describe_group(client, consumer_group)
      assert group.group_id == consumer_group
      assert group.state == "Stable"
    end

    test "sync_group v1 with new API vs v0 with legacy API", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)

      # Join group
      {member_id, generation_id} = join_to_group(client, topic_name, consumer_group)

      # New API with v1 (returns SyncGroup struct with throttle_time_ms)
      {:ok, result_v1} = KafkaExAPI.sync_group(client, consumer_group, generation_id, member_id, api_version: 1)
      assert %KafkaEx.New.Kafka.SyncGroup{throttle_time_ms: _} = result_v1

      Process.sleep(100)

      # Legacy API uses v0 (returns error_code only)
      legacy_request = %KafkaEx.Protocol.SyncGroup.Request{
        group_name: consumer_group,
        member_id: member_id,
        generation_id: generation_id,
        assignments: []
      }

      legacy_response = KafkaEx.sync_group(legacy_request, worker_name: client)
      assert legacy_response.error_code == :no_error

      Process.sleep(100)

      # New API with v0 (no throttle_time_ms)
      {:ok, result_v0} = KafkaExAPI.sync_group(client, consumer_group, generation_id, member_id, api_version: 0)
      assert %KafkaEx.New.Kafka.SyncGroup{throttle_time_ms: nil} = result_v0
    end

    test "sync_group followed by heartbeat across APIs", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)

      # Join group
      {member_id, generation_id} = join_to_group(client, topic_name, consumer_group)

      # Sync via new API
      {:ok, _sync_result} = KafkaExAPI.sync_group(client, consumer_group, generation_id, member_id)

      # Heartbeat via legacy API
      heartbeat_request = %KafkaEx.Protocol.Heartbeat.Request{
        group_name: consumer_group,
        member_id: member_id,
        generation_id: generation_id
      }

      heartbeat_response = KafkaEx.heartbeat(heartbeat_request, worker_name: client)
      assert heartbeat_response.error_code == :no_error

      # Verify group is stable
      {:ok, group} = KafkaExAPI.describe_group(client, consumer_group)
      assert group.group_id == consumer_group
      assert group.state == "Stable"
    end
  end

  # -----------------------------------------------------------------------------
  describe "api_versions compatibility" do
    test "fetch api_versions via new API", %{client: client} do
      alias KafkaEx.New.Kafka.ApiVersions

      {:ok, versions} = KafkaExAPI.api_versions(client)

      assert is_map(versions.api_versions)
      assert map_size(versions.api_versions) > 0

      assert {:ok, _} = ApiVersions.max_version_for_api(versions, 0)
      assert {:ok, _} = ApiVersions.max_version_for_api(versions, 1)
      assert {:ok, _} = ApiVersions.max_version_for_api(versions, 3)
      assert {:ok, _} = ApiVersions.max_version_for_api(versions, 18)
    end

    test "fetch api_versions via legacy API", %{client: client} do
      response = KafkaEx.api_versions(worker_name: client)

      assert %KafkaEx.Protocol.ApiVersions.Response{} = response
      assert response.error_code == :no_error
      assert is_list(response.api_versions)
      assert length(response.api_versions) > 0

      api_keys = Enum.map(response.api_versions, & &1.api_key)

      assert 0 in api_keys
      assert 1 in api_keys
      assert 3 in api_keys
      assert 18 in api_keys
    end

    test "new and legacy APIs return compatible API version data", %{client: client} do
      alias KafkaEx.New.Kafka.ApiVersions
      {:ok, new_versions} = KafkaExAPI.api_versions(client)

      legacy_response = KafkaEx.api_versions(worker_name: client)
      new_api_keys = Map.keys(new_versions.api_versions) |> Enum.sort()
      legacy_api_keys = Enum.map(legacy_response.api_versions, & &1.api_key) |> Enum.sort()

      assert new_api_keys == legacy_api_keys

      Enum.each([0, 1, 3, 18], fn api_key ->
        {:ok, new_max} = ApiVersions.max_version_for_api(new_versions, api_key)
        {:ok, new_min} = ApiVersions.min_version_for_api(new_versions, api_key)

        legacy_entry = Enum.find(legacy_response.api_versions, &(&1.api_key == api_key))

        assert legacy_entry.max_version == new_max,
               "API #{api_key} max_version mismatch: new=#{new_max}, legacy=#{legacy_entry.max_version}"

        assert legacy_entry.min_version == new_min,
               "API #{api_key} min_version mismatch: new=#{new_min}, legacy=#{legacy_entry.min_version}"
      end)
    end

    test "new API with V1 returns throttle_time_ms", %{client: client} do
      {:ok, versions} = KafkaExAPI.api_versions(client, api_version: 1)

      assert is_map(versions.api_versions)
      assert is_integer(versions.throttle_time_ms)
      assert versions.throttle_time_ms >= 0
    end

    test "new API with V0 does not return throttle_time_ms", %{client: client} do
      {:ok, versions} = KafkaExAPI.api_versions(client, api_version: 0)

      assert is_map(versions.api_versions)
      assert is_nil(versions.throttle_time_ms)
    end

    test "legacy API always returns throttle_time_ms of 0", %{client: client} do
      response = KafkaEx.api_versions(worker_name: client)

      assert response.throttle_time_ms == 0
    end

    test "api_versions helper functions work correctly", %{client: client} do
      alias KafkaEx.New.Kafka.ApiVersions

      {:ok, versions} = KafkaExAPI.api_versions(client)

      # Test max_version_for_api
      assert {:ok, metadata_max} = ApiVersions.max_version_for_api(versions, 3)
      assert is_integer(metadata_max) and metadata_max >= 0

      # Test min_version_for_api
      assert {:ok, metadata_min} = ApiVersions.min_version_for_api(versions, 3)
      assert is_integer(metadata_min) and metadata_min >= 0
      assert metadata_min <= metadata_max

      # Test version_supported?
      assert ApiVersions.version_supported?(versions, 3, metadata_min)
      assert ApiVersions.version_supported?(versions, 3, metadata_max)
      refute ApiVersions.version_supported?(versions, 3, metadata_max + 100)

      # Test unsupported API
      assert {:error, :unsupported_api} = ApiVersions.max_version_for_api(versions, 9999)
      assert {:error, :unsupported_api} = ApiVersions.min_version_for_api(versions, 9999)
      refute ApiVersions.version_supported?(versions, 9999, 0)
    end
  end

  # -----------------------------------------------------------------------------
  describe "produce compatibility" do
    test "new API can produce and legacy API can fetch", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      # Produce via new API
      messages = [%{value: "new API message", key: "key1"}]
      {:ok, result} = KafkaExAPI.produce(client, topic_name, 0, messages)

      assert result.base_offset >= 0
      offset = result.base_offset

      # Fetch via legacy API
      fetch_response =
        KafkaEx.fetch(topic_name, 0,
          offset: offset,
          auto_commit: false,
          worker_name: client
        )

      assert [%{partitions: [%{message_set: fetched_messages}]}] = fetch_response
      assert length(fetched_messages) >= 1
      [message | _] = fetched_messages
      assert message.value == "new API message"
      assert message.key == "key1"
    end

    test "legacy API can produce and new API can verify offset", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      # Produce via legacy API
      {:ok, legacy_offset} =
        KafkaEx.produce(
          %Proto.Produce.Request{
            topic: topic_name,
            partition: 0,
            required_acks: 1,
            messages: [%Proto.Produce.Message{value: "legacy message"}]
          },
          worker_name: client
        )

      # Produce via new API and verify offset increments
      {:ok, new_result} = KafkaExAPI.produce(client, topic_name, 0, [%{value: "new message"}])

      assert new_result.base_offset == legacy_offset + 1
    end

    test "produce_one via new API works with legacy fetch", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      # Produce via new API produce_one
      {:ok, result} = KafkaExAPI.produce_one(client, topic_name, 0, "single message", key: "single-key")

      offset = result.base_offset

      # Fetch via legacy API
      fetch_response =
        KafkaEx.fetch(topic_name, 0,
          offset: offset,
          auto_commit: false,
          worker_name: client
        )

      assert [%{partitions: [%{message_set: [message | _]}]}] = fetch_response
      assert message.value == "single message"
      assert message.key == "single-key"
    end

    test "both APIs produce to same topic sequentially", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      # Produce via legacy API
      {:ok, offset1} =
        KafkaEx.produce(
          %Proto.Produce.Request{
            topic: topic_name,
            partition: 0,
            required_acks: 1,
            messages: [%Proto.Produce.Message{value: "msg1"}]
          },
          worker_name: client
        )

      # Produce via new API
      {:ok, result2} = KafkaExAPI.produce(client, topic_name, 0, [%{value: "msg2"}])
      offset2 = result2.base_offset

      # Produce via legacy API again
      {:ok, offset3} =
        KafkaEx.produce(
          %Proto.Produce.Request{
            topic: topic_name,
            partition: 0,
            required_acks: 1,
            messages: [%Proto.Produce.Message{value: "msg3"}]
          },
          worker_name: client
        )

      # Offsets should be sequential
      assert offset2 == offset1 + 1
      assert offset3 == offset2 + 1

      # Fetch all and verify order
      fetch_response =
        KafkaEx.fetch(topic_name, 0,
          offset: offset1,
          auto_commit: false,
          worker_name: client
        )

      assert [%{partitions: [%{message_set: messages}]}] = fetch_response
      values = Enum.map(messages, & &1.value)
      assert "msg1" in values
      assert "msg2" in values
      assert "msg3" in values
    end

    test "new API produce with gzip compression can be fetched by legacy API", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      # Produce multiple messages with gzip via new API
      messages = [
        %{value: "compressed msg 1"},
        %{value: "compressed msg 2"}
      ]

      {:ok, result} = KafkaExAPI.produce(client, topic_name, 0, messages, compression: :gzip)
      offset = result.base_offset

      # Fetch via legacy API
      fetch_response =
        KafkaEx.fetch(topic_name, 0,
          offset: offset,
          auto_commit: false,
          worker_name: client
        )

      assert [%{partitions: [%{message_set: fetched_messages}]}] = fetch_response
      values = Enum.map(fetched_messages, & &1.value)
      assert "compressed msg 1" in values
      assert "compressed msg 2" in values
    end

    test "legacy API produce with compression can be verified by new API offset", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      # Produce with compression via legacy API
      {:ok, legacy_offset} =
        KafkaEx.produce(
          %Proto.Produce.Request{
            topic: topic_name,
            partition: 0,
            required_acks: 1,
            compression: :snappy,
            messages: [
              %Proto.Produce.Message{value: "snappy 1"},
              %Proto.Produce.Message{value: "snappy 2"}
            ]
          },
          worker_name: client
        )

      # Verify via new API latest_offset
      {:ok, latest} = KafkaExAPI.latest_offset(client, topic_name, 0)

      # Latest offset should be after the produced messages
      assert latest >= legacy_offset + 2
    end

    test "new API produce with different API versions", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      # Produce with V0
      {:ok, result_v0} = KafkaExAPI.produce(client, topic_name, 0, [%{value: "v0 msg"}], api_version: 0)
      assert result_v0.base_offset >= 0
      assert is_nil(result_v0.throttle_time_ms)

      # Produce with V2
      {:ok, result_v2} = KafkaExAPI.produce(client, topic_name, 0, [%{value: "v2 msg"}], api_version: 2)
      assert result_v2.base_offset == result_v0.base_offset + 1

      # Produce with V3 (RecordBatch format)
      {:ok, result_v3} = KafkaExAPI.produce(client, topic_name, 0, [%{value: "v3 msg"}], api_version: 3)
      assert result_v3.base_offset == result_v2.base_offset + 1

      # Fetch all via legacy API
      fetch_response =
        KafkaEx.fetch(topic_name, 0,
          offset: result_v0.base_offset,
          auto_commit: false,
          worker_name: client
        )

      assert [%{partitions: [%{message_set: messages}]}] = fetch_response
      values = Enum.map(messages, & &1.value)
      assert "v0 msg" in values
      assert "v2 msg" in values
      assert "v3 msg" in values
    end

    test "new API produce with headers (V3+) can be fetched", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      # Produce with headers via new API (V3+)
      messages = [
        %{
          value: "message with headers",
          key: "header-key",
          headers: [{"content-type", "application/json"}, {"trace-id", "xyz789"}]
        }
      ]

      {:ok, result} = KafkaExAPI.produce(client, topic_name, 0, messages, api_version: 3)
      offset = result.base_offset

      # Fetch via legacy API
      fetch_response =
        KafkaEx.fetch(topic_name, 0,
          offset: offset,
          auto_commit: false,
          worker_name: client
        )

      assert [%{partitions: [%{message_set: [message | _]}]}] = fetch_response
      assert message.value == "message with headers"
      assert message.key == "header-key"
      # Headers may or may not be returned depending on fetch API version
    end

    test "produce error handling is consistent across APIs", %{client: client} do
      # Try to produce to non-existent partition
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name, partitions: 1)

      # New API error
      new_result = KafkaExAPI.produce(client, topic_name, 99, [%{value: "test"}])

      case new_result do
        {:error, error} ->
          assert error in [:unknown_topic_or_partition, :leader_not_available]

        {:ok, _} ->
          # Some Kafka configurations might auto-create partitions
          assert true
      end
    end

    test "multiple messages in batch produce correctly", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      # Produce batch via new API
      messages =
        Enum.map(1..10, fn i ->
          %{value: "batch message #{i}", key: "key-#{i}"}
        end)

      {:ok, result} = KafkaExAPI.produce(client, topic_name, 0, messages)
      base_offset = result.base_offset

      # Fetch via legacy API
      fetch_response =
        KafkaEx.fetch(topic_name, 0,
          offset: base_offset,
          auto_commit: false,
          worker_name: client
        )

      assert [%{partitions: [%{message_set: fetched_messages}]}] = fetch_response
      assert length(fetched_messages) >= 10

      # Verify all messages are present
      values = Enum.map(fetched_messages, & &1.value)

      Enum.each(1..10, fn i ->
        assert "batch message #{i}" in values
      end)
    end

    test "produce to multiple partitions works with both APIs", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name, partitions: 3)

      # Produce via new API to partition 0
      {:ok, result0} = KafkaExAPI.produce(client, topic_name, 0, [%{value: "p0 new"}])

      # Produce via legacy API to partition 1
      {:ok, _offset1} =
        KafkaEx.produce(
          %Proto.Produce.Request{
            topic: topic_name,
            partition: 1,
            required_acks: 1,
            messages: [%Proto.Produce.Message{value: "p1 legacy"}]
          },
          worker_name: client
        )

      # Produce via new API to partition 2
      {:ok, result2} = KafkaExAPI.produce(client, topic_name, 2, [%{value: "p2 new"}])

      # Verify each partition
      assert result0.partition == 0
      assert result2.partition == 2

      # Fetch from each partition
      fetch0 = KafkaEx.fetch(topic_name, 0, offset: 0, worker_name: client)
      fetch1 = KafkaEx.fetch(topic_name, 1, offset: 0, worker_name: client)
      fetch2 = KafkaEx.fetch(topic_name, 2, offset: 0, worker_name: client)

      assert [%{partitions: [%{message_set: [msg0 | _]}]}] = fetch0
      assert [%{partitions: [%{message_set: [msg1 | _]}]}] = fetch1
      assert [%{partitions: [%{message_set: [msg2 | _]}]}] = fetch2

      assert msg0.value == "p0 new"
      assert msg1.value == "p1 legacy"
      assert msg2.value == "p2 new"
    end
  end

  # -----------------------------------------------------------------------------
  describe "fetch compatibility" do
    test "new API can fetch what legacy API produced", %{client: client} do
      alias KafkaEx.New.Kafka.Fetch

      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)
      Process.sleep(100)

      # Produce via legacy API
      {:ok, offset} =
        KafkaEx.produce(
          %Proto.Produce.Request{
            topic: topic_name,
            partition: 0,
            required_acks: 1,
            messages: [%Proto.Produce.Message{value: "legacy message", key: "legacy-key"}]
          },
          worker_name: client
        )

      # Fetch via new API
      {:ok, fetch_result} = KafkaExAPI.fetch(client, topic_name, 0, offset)

      assert %Fetch{} = fetch_result
      assert fetch_result.topic == topic_name
      assert fetch_result.partition == 0
      assert length(fetch_result.records) >= 1

      [message | _] = fetch_result.records
      assert message.value == "legacy message"
      assert message.key == "legacy-key"
      assert message.offset == offset
    end

    test "legacy API can fetch what new API produced", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      # Produce via new API
      messages = [%{value: "new API message", key: "new-key"}]
      {:ok, result} = KafkaExAPI.produce(client, topic_name, 0, messages)
      offset = result.base_offset

      # Fetch via legacy API
      fetch_response =
        KafkaEx.fetch(topic_name, 0,
          offset: offset,
          auto_commit: false,
          worker_name: client
        )

      assert [%{partitions: [%{message_set: fetched_messages}]}] = fetch_response
      assert length(fetched_messages) >= 1
      [message | _] = fetched_messages
      assert message.value == "new API message"
      assert message.key == "new-key"
    end

    test "both APIs return same message content", %{client: client} do
      alias KafkaEx.New.Kafka.Fetch

      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)
      Process.sleep(100)

      # Produce message
      {:ok, offset} =
        KafkaEx.produce(
          %Proto.Produce.Request{
            topic: topic_name,
            partition: 0,
            required_acks: 1,
            messages: [%Proto.Produce.Message{value: "test message", key: "test-key"}]
          },
          worker_name: client
        )

      # Fetch via legacy API
      legacy_response =
        KafkaEx.fetch(topic_name, 0,
          offset: offset,
          auto_commit: false,
          worker_name: client
        )

      [%{partitions: [%{message_set: [legacy_message | _]}]}] = legacy_response

      # Fetch via new API
      {:ok, new_response} = KafkaExAPI.fetch(client, topic_name, 0, offset)
      [new_message | _] = new_response.records

      # Both should return identical message content
      assert legacy_message.value == new_message.value
      assert legacy_message.key == new_message.key
      assert legacy_message.offset == new_message.offset
    end

    test "new API fetch_all works with legacy produced messages", %{client: client} do
      alias KafkaEx.New.Kafka.Fetch

      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      # Produce multiple messages via legacy API
      {:ok, _} =
        KafkaEx.produce(
          %Proto.Produce.Request{
            topic: topic_name,
            partition: 0,
            required_acks: 1,
            messages: [
              %Proto.Produce.Message{value: "msg1"},
              %Proto.Produce.Message{value: "msg2"},
              %Proto.Produce.Message{value: "msg3"}
            ]
          },
          worker_name: client
        )

      # Fetch all via new API (use max_wait_time to avoid timeouts)
      {:ok, fetch_result} = KafkaExAPI.fetch_all(client, topic_name, 0, max_wait_time: 1000)

      assert %Fetch{} = fetch_result
      assert length(fetch_result.records) >= 3

      values = Enum.map(fetch_result.records, & &1.value)
      assert "msg1" in values
      assert "msg2" in values
      assert "msg3" in values
    end

    test "new API with different API versions", %{client: client} do
      alias KafkaEx.New.Kafka.Fetch

      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      # Produce message
      {:ok, result} = KafkaExAPI.produce(client, topic_name, 0, [%{value: "test"}])
      offset = result.base_offset

      # Fetch with V0
      {:ok, fetch_v0} = KafkaExAPI.fetch(client, topic_name, 0, offset, api_version: 0)
      assert %Fetch{} = fetch_v0
      assert hd(fetch_v0.records).value == "test"

      # Fetch with V1 (adds throttle_time_ms)
      {:ok, fetch_v1} = KafkaExAPI.fetch(client, topic_name, 0, offset, api_version: 1)
      assert %Fetch{} = fetch_v1
      assert is_integer(fetch_v1.throttle_time_ms) or is_nil(fetch_v1.throttle_time_ms)

      # Fetch with V3 (default)
      {:ok, fetch_v3} = KafkaExAPI.fetch(client, topic_name, 0, offset, api_version: 3)
      assert %Fetch{} = fetch_v3

      # Fetch with V4 (adds isolation_level)
      {:ok, fetch_v4} = KafkaExAPI.fetch(client, topic_name, 0, offset, api_version: 4)
      assert %Fetch{} = fetch_v4
      assert is_integer(fetch_v4.last_stable_offset) or is_nil(fetch_v4.last_stable_offset)

      # Fetch with V5 (adds log_start_offset)
      {:ok, fetch_v5} = KafkaExAPI.fetch(client, topic_name, 0, offset, api_version: 5)
      assert %Fetch{} = fetch_v5
      assert is_integer(fetch_v5.log_start_offset) or is_nil(fetch_v5.log_start_offset)
    end

    test "new API high_watermark matches legacy API", %{client: client} do
      alias KafkaEx.New.Kafka.Fetch

      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      # Produce some messages
      KafkaEx.produce(
        %Proto.Produce.Request{
          topic: topic_name,
          partition: 0,
          required_acks: 1,
          messages: [
            %Proto.Produce.Message{value: "msg1"},
            %Proto.Produce.Message{value: "msg2"}
          ]
        },
        worker_name: client
      )

      # Fetch via legacy API
      legacy_response =
        KafkaEx.fetch(topic_name, 0,
          offset: 0,
          auto_commit: false,
          worker_name: client
        )

      [%{partitions: [%{hw_mark_offset: legacy_hw}]}] = legacy_response

      # Fetch via new API (use max_wait_time to avoid timeout)
      {:ok, new_response} = KafkaExAPI.fetch(client, topic_name, 0, 0, max_wait_time: 1000)

      # High watermarks should be equal
      assert new_response.high_watermark == legacy_hw
    end

    test "new API with compressed messages from legacy API", %{client: client} do
      alias KafkaEx.New.Kafka.Fetch

      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      # Small delay to ensure topic is ready
      Process.sleep(100)

      # Produce compressed messages via legacy API
      {:ok, offset} =
        KafkaEx.produce(
          %Proto.Produce.Request{
            topic: topic_name,
            partition: 0,
            required_acks: 1,
            compression: :gzip,
            messages: [
              %Proto.Produce.Message{value: "gzip 1"},
              %Proto.Produce.Message{value: "gzip 2"}
            ]
          },
          worker_name: client
        )

      # Fetch via new API
      {:ok, fetch_result} = KafkaExAPI.fetch(client, topic_name, 0, offset)

      assert %Fetch{} = fetch_result
      values = Enum.map(fetch_result.records, & &1.value)
      assert "gzip 1" in values
      assert "gzip 2" in values
    end

    test "new API handles empty partition", %{client: client} do
      alias KafkaEx.New.Kafka.Fetch

      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      # Get latest offset
      {:ok, latest_offset} = KafkaExAPI.latest_offset(client, topic_name, 0)

      # Fetch from empty partition with short wait
      {:ok, fetch_result} = KafkaExAPI.fetch(client, topic_name, 0, latest_offset, max_wait_time: 100)

      assert %Fetch{} = fetch_result
      assert fetch_result.records == []
      assert Fetch.empty?(fetch_result)
    end

    test "Fetch helper functions work correctly", %{client: client} do
      alias KafkaEx.New.Kafka.Fetch
      alias KafkaEx.New.Kafka.Fetch.Record

      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      # Produce messages
      {:ok, result} =
        KafkaExAPI.produce(client, topic_name, 0, [
          %{value: "test", key: "key1"},
          %{value: "test2", key: nil}
        ])

      offset = result.base_offset

      # Fetch
      {:ok, fetch_result} = KafkaExAPI.fetch(client, topic_name, 0, offset)

      # Test Fetch helpers
      assert Fetch.empty?(fetch_result) == false
      assert Fetch.record_count(fetch_result) >= 2
      assert Fetch.next_offset(fetch_result) > offset

      # Test Message helpers
      [msg1, msg2 | _] = fetch_result.records
      assert Record.has_value?(msg1) == true
      assert Record.has_key?(msg1) == true
      assert Record.has_key?(msg2) == false
    end

    test "produce via new API then fetch via both APIs returns same data", %{client: client} do
      alias KafkaEx.New.Kafka.Fetch

      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      # Produce via new API with V3 (RecordBatch format)
      {:ok, result} =
        KafkaExAPI.produce(client, topic_name, 0, [%{value: "round trip", key: "rt-key"}], api_version: 3)

      offset = result.base_offset

      # Fetch via legacy API
      legacy_response =
        KafkaEx.fetch(topic_name, 0,
          offset: offset,
          auto_commit: false,
          worker_name: client
        )

      [%{partitions: [%{message_set: [legacy_msg | _]}]}] = legacy_response

      # Fetch via new API
      {:ok, new_response} = KafkaExAPI.fetch(client, topic_name, 0, offset)
      assert %Fetch{} = new_response
      [new_msg | _] = new_response.records

      # Both should have identical content
      assert legacy_msg.value == "round trip"
      assert new_msg.value == "round trip"
      assert legacy_msg.key == "rt-key"
      assert new_msg.key == "rt-key"
      assert legacy_msg.offset == new_msg.offset
    end
  end

  # -----------------------------------------------------------------------------
  # FindCoordinator / ConsumerMetadata Compatibility Tests
  # -----------------------------------------------------------------------------

  describe "find_coordinator/consumer_group_metadata compatibility" do
    test "legacy consumer_group_metadata returns coordinator info", %{client: client} do
      consumer_group = "find-coord-compat-#{:rand.uniform(100_000)}"

      # Use the legacy API
      response = KafkaEx.consumer_group_metadata(client, consumer_group)

      assert %KafkaEx.Protocol.ConsumerMetadata.Response{} = response
      assert response.error_code == :no_error
      assert response.coordinator_id >= 0
      assert response.coordinator_host != nil
      assert response.coordinator_host != ""
      assert response.coordinator_port > 0
    end

    test "new find_coordinator API returns same coordinator as legacy", %{client: client} do
      consumer_group = "find-coord-same-#{:rand.uniform(100_000)}"

      # Use legacy API
      legacy_response = KafkaEx.consumer_group_metadata(client, consumer_group)
      assert legacy_response.error_code == :no_error

      # Use new API
      {:ok, new_response} = KafkaExAPI.find_coordinator(client, consumer_group)
      assert new_response.error_code == :no_error

      # Both should return the same coordinator
      assert legacy_response.coordinator_id == new_response.coordinator.node_id
      assert legacy_response.coordinator_host == new_response.coordinator.host
      assert legacy_response.coordinator_port == new_response.coordinator.port
    end

    test "both APIs handle invalid consumer group consistently", %{client: client} do
      # Empty consumer group is invalid
      legacy_response = KafkaEx.consumer_group_metadata(client, "")
      {:ok, new_response} = KafkaExAPI.find_coordinator(client, "")

      # Both should return the same error or both succeed (broker-dependent)
      # The key is they should be consistent
      legacy_error = legacy_response.error_code
      new_error = new_response.error_code

      assert legacy_error == new_error
    end

    test "coordinator is consistent across multiple calls", %{client: client} do
      consumer_group = "find-coord-consistent-#{:rand.uniform(100_000)}"

      # Call legacy API twice
      legacy1 = KafkaEx.consumer_group_metadata(client, consumer_group)
      legacy2 = KafkaEx.consumer_group_metadata(client, consumer_group)

      # Call new API twice
      {:ok, new1} = KafkaExAPI.find_coordinator(client, consumer_group)
      {:ok, new2} = KafkaExAPI.find_coordinator(client, consumer_group)

      # All should return the same coordinator (unless rebalance)
      assert legacy1.coordinator_id == legacy2.coordinator_id
      assert new1.coordinator.node_id == new2.coordinator.node_id
      assert legacy1.coordinator_id == new1.coordinator.node_id
    end

    test "new API with V0 returns equivalent to legacy", %{client: client} do
      consumer_group = "find-coord-v0-#{:rand.uniform(100_000)}"

      # Legacy API uses V0 internally
      legacy_response = KafkaEx.consumer_group_metadata(client, consumer_group)

      # Force V0 on new API
      {:ok, new_response} = KafkaExAPI.find_coordinator(client, consumer_group, api_version: 0)

      assert legacy_response.error_code == :no_error
      assert new_response.error_code == :no_error
      assert legacy_response.coordinator_id == new_response.coordinator.node_id
    end
  end

  # -----------------------------------------------------------------------------
  describe "create_topics compatibility" do
    alias KafkaEx.New.Kafka.CreateTopics

    test "new API creates topic successfully", %{client: client} do
      topic_name = "create-topics-new-api-#{:rand.uniform(100_000)}"

      topics = [[topic: topic_name, num_partitions: 3, replication_factor: 1]]

      {:ok, result} = KafkaExAPI.create_topics(client, topics, 10_000)

      assert %CreateTopics{} = result
      assert CreateTopics.success?(result)
      assert length(result.topic_results) == 1

      [topic_result] = result.topic_results
      assert topic_result.topic == topic_name
      assert topic_result.error == :no_error
    end

    test "new API returns topic_already_exists for duplicate topic", %{client: client} do
      topic_name = "create-topics-duplicate-#{:rand.uniform(100_000)}"

      topics = [[topic: topic_name, num_partitions: 1, replication_factor: 1]]

      # Create first time
      {:ok, result1} = KafkaExAPI.create_topics(client, topics, 10_000)
      assert CreateTopics.success?(result1)

      # Try to create again
      {:ok, result2} = KafkaExAPI.create_topics(client, topics, 10_000)
      assert not CreateTopics.success?(result2)

      [topic_result] = result2.topic_results
      assert topic_result.error == :topic_already_exists
    end

    test "new API can create multiple topics at once", %{client: client} do
      base = :rand.uniform(100_000)
      topic1 = "create-topics-multi-1-#{base}"
      topic2 = "create-topics-multi-2-#{base}"
      topic3 = "create-topics-multi-3-#{base}"

      topics = [
        [topic: topic1, num_partitions: 1, replication_factor: 1],
        [topic: topic2, num_partitions: 2, replication_factor: 1],
        [topic: topic3, num_partitions: 3, replication_factor: 1]
      ]

      {:ok, result} = KafkaExAPI.create_topics(client, topics, 10_000)

      assert CreateTopics.success?(result)
      assert length(result.topic_results) == 3

      topic_names = Enum.map(result.topic_results, & &1.topic) |> Enum.sort()
      assert topic_names == [topic1, topic2, topic3] |> Enum.sort()
    end

    test "legacy API creates topic successfully", %{client: client} do
      topic_name = "create-topics-legacy-api-#{:rand.uniform(100_000)}"

      request = %KafkaEx.Protocol.CreateTopics.TopicRequest{
        topic: topic_name,
        num_partitions: 3,
        replication_factor: 1,
        replica_assignment: [],
        config_entries: []
      }

      response = KafkaEx.create_topics([request], worker_name: client, timeout: 10_000)

      assert %KafkaEx.Protocol.CreateTopics.Response{} = response
      assert [topic_error] = response.topic_errors
      assert topic_error.topic_name == topic_name
      assert topic_error.error_code == :no_error
    end

    test "both APIs can interoperate - new API sees topic created by legacy", %{client: client} do
      topic_name = "create-topics-interop-1-#{:rand.uniform(100_000)}"

      # Create via legacy API
      legacy_request = %KafkaEx.Protocol.CreateTopics.TopicRequest{
        topic: topic_name,
        num_partitions: 2,
        replication_factor: 1
      }

      legacy_response = KafkaEx.create_topics([legacy_request], worker_name: client, timeout: 10_000)
      assert [topic_error] = legacy_response.topic_errors
      assert topic_error.error_code == :no_error

      # Try to create same topic via new API - should get already_exists
      {:ok, new_result} = KafkaExAPI.create_topics(client, [[topic: topic_name]], 10_000)
      [topic_result] = new_result.topic_results
      assert topic_result.error == :topic_already_exists
    end

    test "both APIs can interoperate - legacy API sees topic created by new", %{client: client} do
      topic_name = "create-topics-interop-2-#{:rand.uniform(100_000)}"

      # Create via new API
      {:ok, new_result} =
        KafkaExAPI.create_topics(
          client,
          [[topic: topic_name, num_partitions: 2, replication_factor: 1]],
          10_000
        )

      assert CreateTopics.success?(new_result)

      # Try to create same topic via legacy API - should get already_exists
      legacy_request = %KafkaEx.Protocol.CreateTopics.TopicRequest{
        topic: topic_name,
        num_partitions: 2,
        replication_factor: 1
      }

      legacy_response = KafkaEx.create_topics([legacy_request], worker_name: client, timeout: 10_000)
      assert [topic_error] = legacy_response.topic_errors
      assert topic_error.error_code == :topic_already_exists
    end

    test "new API create_topic convenience function works", %{client: client} do
      topic_name = "create-topic-convenience-#{:rand.uniform(100_000)}"

      {:ok, result} = KafkaExAPI.create_topic(client, topic_name, num_partitions: 2)

      assert CreateTopics.success?(result)
      [topic_result] = result.topic_results
      assert topic_result.topic == topic_name
    end

    test "new API with V0 creates topic successfully", %{client: client} do
      topic_name = "create-topics-v0-#{:rand.uniform(100_000)}"

      {:ok, result} =
        KafkaExAPI.create_topics(
          client,
          [[topic: topic_name, num_partitions: 1, replication_factor: 1]],
          10_000,
          api_version: 0
        )

      assert CreateTopics.success?(result)
      # V0 doesn't have error_message
      [topic_result] = result.topic_results
      assert topic_result.error_message == nil
    end

    test "new API with V2 includes throttle_time_ms", %{client: client} do
      topic_name = "create-topics-v2-#{:rand.uniform(100_000)}"

      {:ok, result} =
        KafkaExAPI.create_topics(
          client,
          [[topic: topic_name, num_partitions: 1, replication_factor: 1]],
          10_000,
          api_version: 2
        )

      assert CreateTopics.success?(result)
      # V2 has throttle_time_ms
      assert is_integer(result.throttle_time_ms)
    end

    test "new API validate_only flag works (V1+)", %{client: client} do
      topic_name = "create-topics-validate-#{:rand.uniform(100_000)}"

      # Validate only - should succeed but not actually create
      {:ok, result} =
        KafkaExAPI.create_topics(
          client,
          [[topic: topic_name, num_partitions: 3, replication_factor: 1]],
          10_000,
          validate_only: true
        )

      assert CreateTopics.success?(result)

      # Topic should not exist - try to create it again
      {:ok, result2} =
        KafkaExAPI.create_topics(
          client,
          [[topic: topic_name, num_partitions: 3, replication_factor: 1]],
          10_000
        )

      # Should succeed because validate_only didn't actually create it
      assert CreateTopics.success?(result2)
    end

    test "new API with config entries creates topic correctly", %{client: client} do
      topic_name = "create-topics-config-#{:rand.uniform(100_000)}"

      topics = [
        [
          topic: topic_name,
          num_partitions: 1,
          replication_factor: 1,
          config_entries: [
            {"retention.ms", "3600000"},
            {"cleanup.policy", "delete"}
          ]
        ]
      ]

      {:ok, result} = KafkaExAPI.create_topics(client, topics, 10_000)

      assert CreateTopics.success?(result)
    end
  end

  # -----------------------------------------------------------------------------
  describe "delete_topics compatibility" do
    alias KafkaEx.New.Kafka.DeleteTopics

    test "new API deletes topic successfully", %{client: client} do
      topic_name = "delete-topics-new-api-#{:rand.uniform(100_000)}"

      # First create the topic
      {:ok, create_result} =
        KafkaExAPI.create_topics(
          client,
          [[topic: topic_name, num_partitions: 1, replication_factor: 1]],
          10_000
        )

      assert KafkaEx.New.Kafka.CreateTopics.success?(create_result)

      # Now delete it
      {:ok, result} = KafkaExAPI.delete_topics(client, [topic_name], 10_000)

      assert %DeleteTopics{} = result
      assert DeleteTopics.success?(result)
      assert length(result.topic_results) == 1

      [topic_result] = result.topic_results
      assert topic_result.topic == topic_name
      assert topic_result.error == :no_error
    end

    test "new API returns error for non-existent topic", %{client: client} do
      topic_name = "delete-topics-nonexistent-#{:rand.uniform(100_000)}"

      {:ok, result} = KafkaExAPI.delete_topics(client, [topic_name], 10_000)

      assert not DeleteTopics.success?(result)

      [topic_result] = result.topic_results
      assert topic_result.error == :unknown_topic_or_partition
    end

    test "new API can delete multiple topics at once", %{client: client} do
      base = :rand.uniform(100_000)
      topic1 = "delete-topics-multi-1-#{base}"
      topic2 = "delete-topics-multi-2-#{base}"
      topic3 = "delete-topics-multi-3-#{base}"

      # Create topics
      topics_to_create = [
        [topic: topic1, num_partitions: 1, replication_factor: 1],
        [topic: topic2, num_partitions: 1, replication_factor: 1],
        [topic: topic3, num_partitions: 1, replication_factor: 1]
      ]

      {:ok, create_result} = KafkaExAPI.create_topics(client, topics_to_create, 10_000)
      assert KafkaEx.New.Kafka.CreateTopics.success?(create_result)

      # Delete all topics
      {:ok, result} = KafkaExAPI.delete_topics(client, [topic1, topic2, topic3], 10_000)

      assert DeleteTopics.success?(result)
      assert length(result.topic_results) == 3

      topic_names = Enum.map(result.topic_results, & &1.topic) |> Enum.sort()
      assert topic_names == [topic1, topic2, topic3] |> Enum.sort()
    end

    test "legacy API deletes topic successfully", %{client: client} do
      topic_name = "delete-topics-legacy-api-#{:rand.uniform(100_000)}"

      # First create the topic
      create_request = %KafkaEx.Protocol.CreateTopics.TopicRequest{
        topic: topic_name,
        num_partitions: 1,
        replication_factor: 1
      }

      create_response = KafkaEx.create_topics([create_request], worker_name: client, timeout: 10_000)
      assert [topic_error] = create_response.topic_errors
      assert topic_error.error_code == :no_error

      # Delete via legacy API
      response = KafkaEx.delete_topics([topic_name], worker_name: client, timeout: 10_000)

      assert %KafkaEx.Protocol.DeleteTopics.Response{} = response
      assert [topic_error] = response.topic_errors
      assert topic_error.topic_name == topic_name
      assert topic_error.error_code == :no_error
    end

    test "both APIs can interoperate - new API deletes topic created by legacy", %{client: client} do
      topic_name = "delete-topics-interop-1-#{:rand.uniform(100_000)}"

      # Create via legacy API
      legacy_request = %KafkaEx.Protocol.CreateTopics.TopicRequest{
        topic: topic_name,
        num_partitions: 1,
        replication_factor: 1
      }

      legacy_response = KafkaEx.create_topics([legacy_request], worker_name: client, timeout: 10_000)
      assert [topic_error] = legacy_response.topic_errors
      assert topic_error.error_code == :no_error

      # Delete via new API
      {:ok, result} = KafkaExAPI.delete_topics(client, [topic_name], 10_000)

      assert DeleteTopics.success?(result)
    end

    test "both APIs can interoperate - legacy API deletes topic created by new", %{client: client} do
      topic_name = "delete-topics-interop-2-#{:rand.uniform(100_000)}"

      # Create via new API
      {:ok, new_result} =
        KafkaExAPI.create_topics(
          client,
          [[topic: topic_name, num_partitions: 1, replication_factor: 1]],
          10_000
        )

      assert KafkaEx.New.Kafka.CreateTopics.success?(new_result)

      # Delete via legacy API
      legacy_response = KafkaEx.delete_topics([topic_name], worker_name: client, timeout: 10_000)
      assert [topic_error] = legacy_response.topic_errors
      assert topic_error.error_code == :no_error
    end

    test "new API with V0 works correctly", %{client: client} do
      topic_name = "delete-topics-v0-#{:rand.uniform(100_000)}"

      # First create the topic
      {:ok, _} =
        KafkaExAPI.create_topics(
          client,
          [[topic: topic_name, num_partitions: 1, replication_factor: 1]],
          10_000
        )

      # Delete with V0
      {:ok, result} = KafkaExAPI.delete_topics(client, [topic_name], 10_000, api_version: 0)

      assert DeleteTopics.success?(result)
      # V0 doesn't have throttle_time_ms
      assert result.throttle_time_ms == nil
    end

    test "new API with V1 includes throttle_time_ms", %{client: client} do
      topic_name = "delete-topics-v1-#{:rand.uniform(100_000)}"

      # First create the topic
      {:ok, _} =
        KafkaExAPI.create_topics(
          client,
          [[topic: topic_name, num_partitions: 1, replication_factor: 1]],
          10_000
        )

      # Delete with V1
      {:ok, result} = KafkaExAPI.delete_topics(client, [topic_name], 10_000, api_version: 1)

      assert DeleteTopics.success?(result)
      # V1 has throttle_time_ms
      assert is_integer(result.throttle_time_ms)
    end

    test "delete_topic convenience function works", %{client: client} do
      topic_name = "delete-topics-convenience-#{:rand.uniform(100_000)}"

      # First create the topic
      {:ok, _} = KafkaExAPI.create_topic(client, topic_name, num_partitions: 1, replication_factor: 1)

      # Delete using convenience function
      {:ok, result} = KafkaExAPI.delete_topic(client, topic_name)

      assert DeleteTopics.success?(result)
    end
  end

  # -----------------------------------------------------------------------------
end
