defmodule KafkaEx.Kafka4.ApiVersionsTest do
  @moduledoc """
  Against a Kafka 4.4.0 KRaft broker: the KIP-848-era versions of APIs KafkaEx
  already implements (Metadata v13, OffsetCommit v10, OffsetFetch v10,
  FindCoordinator v6) are negotiated and round-trip through `KafkaEx.API`, and
  the classic consumer-group flow works end to end on them.
  """
  use ExUnit.Case, async: false

  @moduletag :kafka4
  @moduletag timeout: 300_000

  import KafkaEx.TestHelpers, only: [generate_random_string: 0]

  alias KafkaEx.API
  alias KafkaEx.Client.State
  alias KafkaEx.Cluster.ClusterMetadata
  alias KafkaEx.Cluster.Topic
  alias KafkaEx.Consumer.ConsumerGroup
  alias KafkaEx.Kafka4TestHelpers
  alias KafkaEx.Messages.FindCoordinator
  alias KafkaEx.Messages.Offset
  alias KafkaEx.Messages.Offset.PartitionOffset
  alias KafkaEx.TestSupport.ConsumerGroupHelpers

  defmodule RecordingConsumer do
    @moduledoc false
    # Reports every record as {offset, value} to the test process, tagged with
    # the :tag from extra_consumer_args, so a test can tell consumer groups apart.
    use KafkaEx.Consumer.GenConsumer

    alias KafkaEx.Messages.Fetch.Record

    @impl true
    def init(_topic, _partition), do: {:ok, %{test_pid: nil, tag: nil}}

    @impl true
    def init(_topic, _partition, %{test_pid: pid, tag: tag}), do: {:ok, %{test_pid: pid, tag: tag}}

    @impl true
    def handle_message_set(records, state) do
      send(state.test_pid, {:records, state.tag, Enum.map(records, fn %Record{offset: o, value: v} -> {o, v} end)})
      {:async_commit, state}
    end
  end

  setup_all do
    {:ok, ctx} = Kafka4TestHelpers.start_kafka()
    on_exit(fn -> Kafka4TestHelpers.stop_kafka(ctx) end)
    {:ok, ctx}
  end

  setup ctx do
    {:ok, client} = Kafka4TestHelpers.start_client(ctx)
    on_exit(fn -> KafkaEx.TestSupport.ProcessHelpers.stop_safely(client) end)

    handler = "kafka4-versions-#{inspect(self())}"
    test_pid = self()

    :telemetry.attach(
      handler,
      [:kafka_ex, :request, :start],
      fn _event, _measurements, %{operation: op, api_version: vsn}, _ -> send(test_pid, {:request, op, vsn}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)
    {:ok, Map.put(ctx, :client, client)}
  end

  defp create_topic!(client, partitions \\ 1) do
    topic = "kafka4_" <> generate_random_string()

    {:ok, _} =
      API.create_topic(client, topic, num_partitions: partitions, replication_factor: 1)

    wait_until(fn ->
      match?(
        {:ok, %ClusterMetadata{topics: %{^topic => %Topic{topic_id: <<_::128>>}}}},
        API.metadata(client, [topic], [])
      )
    end)

    topic
  end

  defp wait_until(fun, attempts \\ 40) do
    cond do
      fun.() -> :ok
      attempts == 0 -> flunk("condition not met")
      true -> Process.sleep(250) && wait_until(fun, attempts - 1)
    end
  end

  defp negotiated(client, api) do
    state = :sys.get_state(client)
    {:ok, vsn} = State.max_supported_api_version(state, api)
    vsn
  end

  defp flush_requests do
    receive do
      {:request, _, _} -> flush_requests()
    after
      0 -> :ok
    end
  end

  defp assert_sent(op, vsn) do
    assert_received {:request, ^op, ^vsn}
  end

  test "negotiates the new maxima against Kafka 4.4.0", %{client: client} do
    assert negotiated(client, :metadata) == 13
    assert negotiated(client, :offset_commit) == 10
    assert negotiated(client, :offset_fetch) == 10
    assert negotiated(client, :find_coordinator) == 6
  end

  describe "Metadata v13" do
    test "returns topic ids and the id -> name lookup works", %{client: client} do
      topic = create_topic!(client)
      flush_requests()

      assert {:ok, %ClusterMetadata{} = metadata} = API.metadata(client, [topic], [])
      assert_sent(:metadata, 13)

      assert %Topic{topic_id: <<_::128>> = topic_id} = metadata.topics[topic]
      refute topic_id == <<0::128>>
      assert ClusterMetadata.topic_name_by_id(metadata, topic_id) == {:ok, topic}

      # The client's cached metadata carries the id too.
      {:ok, cached} = API.cluster_metadata(client)
      assert ClusterMetadata.topic_name_by_id(cached, topic_id) == {:ok, topic}
    end

    test "looks topics up by id", %{client: client} do
      topic = create_topic!(client, 2)
      {:ok, %ClusterMetadata{topics: %{^topic => %Topic{topic_id: topic_id}}}} = API.metadata(client, [topic], [])
      flush_requests()

      assert {:ok, %ClusterMetadata{topics: topics}} = API.metadata(client, nil, topic_ids: [topic_id])
      assert_sent(:metadata, 13)
      assert %Topic{name: ^topic, topic_id: ^topic_id, partitions: partitions} = topics[topic]
      assert length(partitions) == 2
      assert map_size(topics) == 1
    end

    test "an unknown topic id is not returned", %{client: client} do
      assert {:ok, %ClusterMetadata{topics: topics}} =
               API.metadata(client, nil, topic_ids: [:crypto.strong_rand_bytes(16)])

      assert topics == %{}
    end
  end

  describe "FindCoordinator v6" do
    test "finds the group coordinator", %{client: client} do
      flush_requests()
      assert {:ok, %FindCoordinator{} = result} = API.find_coordinator(client, "kafka4_" <> generate_random_string())
      assert_sent(:find_coordinator, 6)
      assert FindCoordinator.success?(result)
      assert FindCoordinator.coordinator_node_id(result) == 1
    end
  end

  describe "OffsetCommit / OffsetFetch v10" do
    test "round-trip a standalone commit by topic id", %{client: client} do
      topic = create_topic!(client)
      group = "kafka4_" <> generate_random_string()
      flush_requests()

      assert {:ok, [%Offset{topic: ^topic, partition_offsets: [%PartitionOffset{partition: 0, error_code: :no_error}]}]} =
               API.commit_offset(client, group, topic, [%{partition_num: 0, offset: 17, metadata: "md"}])

      assert_sent(:offset_commit, 10)

      assert {:ok,
              [%Offset{topic: ^topic, partition_offsets: [%PartitionOffset{partition: 0, offset: 17, metadata: "md"}]}]} =
               API.fetch_committed_offset(client, group, topic, [%{partition_num: 0}])

      assert_sent(:offset_fetch, 10)
    end

    test "v9 (by name) still round-trips when pinned", %{client: client} do
      topic = create_topic!(client)
      group = "kafka4_" <> generate_random_string()
      flush_requests()

      assert {:ok, _} = API.commit_offset(client, group, topic, [%{partition_num: 0, offset: 3}], api_version: 9)
      assert_sent(:offset_commit, 9)

      assert {:ok, [%Offset{partition_offsets: [%PartitionOffset{offset: 3}]}]} =
               API.fetch_committed_offset(client, group, topic, [%{partition_num: 0}], api_version: 9)

      assert_sent(:offset_fetch, 9)
    end

    test "round-trip for a classic group member (generation + member id)", %{client: client} do
      topic = create_topic!(client)
      group = "kafka4_" <> generate_random_string()

      {:ok, %{member_id: member_id, generation_id: generation_id}} = join_classic_group(client, group, topic)
      flush_requests()

      assert {:ok, [%Offset{topic: ^topic}]} =
               API.commit_offset(client, group, topic, [%{partition_num: 0, offset: 99}],
                 member_id: member_id,
                 generation_id: generation_id
               )

      assert_sent(:offset_commit, 10)

      assert {:ok, [%Offset{partition_offsets: [%PartitionOffset{offset: 99}]}]} =
               API.fetch_committed_offset(client, group, topic, [%{partition_num: 0}])

      assert_sent(:offset_fetch, 10)

      # A stale generation is rejected the same way as on older versions.
      assert {:error, :illegal_generation} =
               API.commit_offset(client, group, topic, [%{partition_num: 0, offset: 100}],
                 member_id: member_id,
                 generation_id: generation_id + 1
               )

      {:ok, _} = API.leave_group(client, group, member_id)
    end
  end

  describe "classic consumer group (GenConsumer/ConsumerGroup)" do
    test "consumes, commits and resumes from the committed offset on the new versions",
         %{client: client} = ctx do
      topic = create_topic!(client)
      group = "kafka4_cg_" <> generate_random_string()

      Enum.each(1..5, &API.produce(client, topic, 0, [%{value: "msg-#{&1}"}]))

      # commit_threshold: 1 -> every acked message set commits (async commits
      # otherwise wait for the next message set after commit_interval).
      opts = fn tag ->
        Kafka4TestHelpers.consumer_group_opts(ctx,
          commit_threshold: 1,
          extra_consumer_args: %{test_pid: self(), tag: tag}
        )
      end

      {:ok, cg} = ConsumerGroup.start_link(RecordingConsumer, group, [topic], opts.(:first))
      on_exit(fn -> ConsumerGroupHelpers.stop_consumer_group(cg) end)

      assert {:ok, :active} = ConsumerGroupHelpers.wait_for_active(cg, timeout: 60_000)

      assert [{0, "msg-1"}, {1, "msg-2"}, {2, "msg-3"}, {3, "msg-4"}, {4, "msg-5"}] =
               collect_records(:first, 5)

      # Committed through OffsetCommit v10.
      assert_receive {:request, :offset_commit, 10}, 10_000

      wait_until(fn ->
        match?(
          {:ok, [%Offset{partition_offsets: [%PartitionOffset{offset: 5}]}]},
          API.fetch_committed_offset(client, group, topic, [%{partition_num: 0}])
        )
      end)

      # Wait for the first group to be fully gone before producing more, so it
      # cannot consume msg-6/msg-7 itself. The Manager is given 25s to leave the
      # group on shutdown, so the default 5s stop timeout is not enough.
      stop_and_await_down(cg)

      Enum.each(6..7, &API.produce(client, topic, 0, [%{value: "msg-#{&1}"}]))
      flush_records()
      flush_requests()

      {:ok, cg2} = ConsumerGroup.start_link(RecordingConsumer, group, [topic], opts.(:second))
      on_exit(fn -> ConsumerGroupHelpers.stop_consumer_group(cg2) end)
      assert {:ok, :active} = ConsumerGroupHelpers.wait_for_active(cg2, timeout: 60_000)

      # The second group resumes at the committed offset 5: it sees exactly
      # msg-6 and msg-7, and nothing it already consumed.
      assert [{5, "msg-6"}, {6, "msg-7"}] = collect_records(:second, 2)
      refute_receive {:records, _, _}, 2_000

      # Its committed offsets were loaded through OffsetFetch v10, and the group
      # client negotiated the other new versions (telemetry is global, and the
      # test client sent nothing since the flush).
      assert_received {:request, :offset_fetch, 10}
      assert_received {:request, :find_coordinator, 6}
      assert_received {:request, :metadata, 13}
    end
  end

  defp stop_and_await_down(pid) do
    ref = Process.monitor(pid)
    Process.unlink(pid)
    KafkaEx.TestSupport.ProcessHelpers.stop_safely(pid, :normal, 60_000)
    assert_receive {:DOWN, ^ref, :process, ^pid, _}, 60_000
  end

  # Records reported by RecordingConsumer for the consumer group tagged `tag`, in
  # offset order, until `count` have arrived.
  defp collect_records(tag, count, acc \\ []) do
    if length(acc) >= count do
      Enum.sort(acc)
    else
      receive do
        {:records, ^tag, records} -> collect_records(tag, count, acc ++ records)
      after
        30_000 -> flunk("expected #{count} records, got #{inspect(Enum.sort(acc))}")
      end
    end
  end

  defp flush_records do
    receive do
      {:records, _, _} -> flush_records()
    after
      0 -> :ok
    end
  end

  defp join_classic_group(client, group, topic) do
    alias KafkaEx.Test.KayrockFixtures, as: Fixtures

    metadata = Fixtures.group_protocol_metadata(topics: [topic])

    join_opts = [
      session_timeout: 30_000,
      rebalance_timeout: 30_000,
      group_protocols: [%{name: "assign", metadata: metadata}]
    ]

    {:ok, join} = API.join_group(client, group, "", join_opts)

    assignment =
      Fixtures.member_assignment(partition_assignments: [Fixtures.partition_assignment(topic: topic, partitions: [0])])

    {:ok, _} =
      API.sync_group(client, group, join.generation_id, join.member_id,
        group_assignment: [%{member_id: join.member_id, assignment: assignment}]
      )

    {:ok, %{member_id: join.member_id, generation_id: join.generation_id}}
  end
end
