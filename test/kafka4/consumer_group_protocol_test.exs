defmodule KafkaEx.Kafka4.ConsumerGroupProtocolTest do
  @moduledoc """
  Against a Kafka 4.4.0 KRaft broker: the KIP-848 consumer group protocol APIs,
  ConsumerGroupHeartbeat and ConsumerGroupDescribe, driven through
  `KafkaEx.API` (join -> assignment by topic id -> heartbeat -> describe ->
  second member and redistribution -> leave), plus the error paths.
  """
  use ExUnit.Case, async: false

  @moduletag :kafka4
  @moduletag timeout: 300_000

  import KafkaEx.TestHelpers, only: [generate_random_string: 0]

  alias KafkaEx.API
  alias KafkaEx.Client.State
  alias KafkaEx.Cluster.ClusterMetadata
  alias KafkaEx.Cluster.Topic
  alias KafkaEx.Kafka4TestHelpers
  alias KafkaEx.Messages.ConsumerGroupDescription
  alias KafkaEx.Messages.ConsumerGroupDescription.Member
  alias KafkaEx.Messages.ConsumerGroupDescription.Member.MemberAssignment.PartitionAssignment
  alias KafkaEx.Messages.ConsumerGroupHeartbeat
  alias KafkaEx.Messages.TopicPartitions

  @partitions 4

  defmodule ForwardingConsumer do
    @moduledoc false
    # Sends {:consumed, pid, [offset]} to the test process.
    use KafkaEx.Consumer.GenConsumer

    def init(_topic, _partition, %{test_pid: test_pid}), do: {:ok, test_pid}

    def handle_message_set(records, test_pid) do
      send(test_pid, {:consumed, self(), Enum.map(records, & &1.offset)})
      {:sync_commit, test_pid}
    end
  end

  defmodule FakeManager do
    @moduledoc false
    use GenServer
    def start_link(test_pid), do: GenServer.start_link(__MODULE__, test_pid)
    def init(test_pid), do: {:ok, test_pid}

    def handle_cast(msg, test_pid) do
      send(test_pid, {:manager_received, msg})
      {:noreply, test_pid}
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
    {:ok, Map.put(ctx, :client, client)}
  end

  defp create_topic!(client, partitions) do
    topic = "kafka4_cg_" <> generate_random_string()
    {:ok, _} = API.create_topic(client, topic, num_partitions: partitions, replication_factor: 1)

    wait_until(fn ->
      match?(
        {:ok, %ClusterMetadata{topics: %{^topic => %Topic{topic_id: <<_::128>>, partitions: [_ | _]}}}},
        API.metadata(client, [topic], [])
      )
    end)

    topic
  end

  defp wait_until(fun, attempts \\ 60) do
    cond do
      fun.() -> :ok
      attempts == 0 -> flunk("condition not met")
      true -> Process.sleep(250) && wait_until(fun, attempts - 1)
    end
  end

  # A minimal KIP-848 member: joins, then on every heartbeat reports what it
  # owns; a new assignment is adopted immediately (revocation is instant here).
  defp join!(client, group, topic) do
    {:ok, %ConsumerGroupHeartbeat{} = result} =
      API.consumer_group_heartbeat(client, group,
        member_epoch: 0,
        subscribed_topic_names: [topic],
        topic_partitions: [],
        rebalance_timeout_ms: 30_000,
        server_assignor: "uniform"
      )

    adopt(%{id: result.member_id, epoch: 0, owned: []}, result)
  end

  defp heartbeat!(client, group, member) do
    {:ok, result} =
      API.consumer_group_heartbeat(client, group,
        member_id: member.id,
        member_epoch: member.epoch,
        topic_partitions: member.owned
      )

    adopt(member, result)
  end

  defp adopt(member, %ConsumerGroupHeartbeat{member_id: id, member_epoch: epoch, assignment: assignment}) do
    owned = if is_nil(assignment), do: member.owned, else: assignment
    %{member | id: id || member.id, epoch: epoch, owned: owned}
  end

  defp partitions_of(%{owned: owned}), do: owned |> Enum.flat_map(& &1.partitions) |> Enum.sort()

  # Heartbeats every member in turn until `done?` holds for the list of members.
  defp converge(client, group, members, done?, rounds \\ 60) do
    members = Enum.map(members, &heartbeat!(client, group, &1))

    cond do
      done?.(members) -> members
      rounds == 0 -> flunk("group did not converge: #{inspect(Enum.map(members, &partitions_of/1))}")
      true -> Process.sleep(200) && converge(client, group, members, done?, rounds - 1)
    end
  end

  defp negotiated(client, api) do
    {:ok, vsn} = client |> :sys.get_state() |> State.max_supported_api_version(api)
    vsn
  end

  test "negotiates ConsumerGroupHeartbeat v1 and ConsumerGroupDescribe v1", %{client: client} do
    assert negotiated(client, :consumer_group_heartbeat) == 1
    assert negotiated(client, :consumer_group_describe) == 1
  end

  test "join, assignment by topic id, heartbeat, describe, redistribution and leave", %{client: client} do
    topic = create_topic!(client, @partitions)
    group = "kafka4_cg_" <> generate_random_string()
    {:ok, %ClusterMetadata{} = metadata} = API.cluster_metadata(client)
    topic_id = metadata.topics[topic].topic_id

    # --- join (epoch 0, client-generated member id per KIP-1082) ---
    a = join!(client, group, topic)
    assert byte_size(a.id) == 22
    assert a.epoch > 0

    a = converge(client, group, [a], fn [a] -> partitions_of(a) == Enum.to_list(0..(@partitions - 1)) end) |> hd()

    # assignment names topics by id; the client resolves the name from its metadata
    assert [%TopicPartitions{topic_id: ^topic_id, topic_name: ^topic}] = a.owned
    assert {:ok, ^topic} = ClusterMetadata.topic_name_by_id(metadata, topic_id)

    # --- steady-state heartbeat: same epoch, assignment unchanged (nil) ---
    {:ok, steady} = API.consumer_group_heartbeat(client, group, member_id: a.id, member_epoch: a.epoch)
    assert steady.member_epoch == a.epoch
    assert steady.assignment == nil
    assert steady.heartbeat_interval_ms > 0

    # --- describe shows the member and its assignment ---
    assert {:ok, %ConsumerGroupDescription{} = desc} = API.describe_consumer_group(client, group)
    assert desc.group_id == group
    assert desc.type == :consumer
    assert desc.state == "Stable"
    assert desc.protocol == "uniform"
    assert desc.group_epoch >= 1
    assert [%Member{} = member] = desc.members
    assert member.member_id == a.id
    assert member.member_epoch == a.epoch
    assert member.member_type == :consumer
    assert member.subscribed_topic_names == [topic]

    assert [%PartitionAssignment{topic: ^topic, topic_id: ^topic_id, partitions: current}] =
             member.member_assignment.partition_assignments

    assert Enum.sort(current) == Enum.to_list(0..(@partitions - 1))

    # --- a second member joins; partitions are redistributed across heartbeats ---
    b = join!(client, group, topic)
    refute b.id == a.id

    [a, b] =
      converge(client, group, [a, b], fn [a, b] ->
        pa = partitions_of(a)
        pb = partitions_of(b)
        pa != [] and pb != [] and Enum.sort(pa ++ pb) == Enum.to_list(0..(@partitions - 1))
      end)

    assert MapSet.disjoint?(MapSet.new(partitions_of(a)), MapSet.new(partitions_of(b)))

    {:ok, desc} = API.describe_consumer_group(client, group)
    assert desc.members |> Enum.map(& &1.member_id) |> Enum.sort() == Enum.sort([a.id, b.id])

    # --- b leaves with epoch -1 ---
    assert {:ok, %ConsumerGroupHeartbeat{member_epoch: -1}} =
             API.consumer_group_heartbeat(client, group, member_id: b.id, member_epoch: -1)

    {:ok, desc} = API.describe_consumer_group(client, group)
    assert Enum.map(desc.members, & &1.member_id) == [a.id]

    # a gets the partitions back
    [a] = converge(client, group, [a], fn [a] -> partitions_of(a) == Enum.to_list(0..(@partitions - 1)) end)

    # --- a heartbeat with a wrong epoch is fenced ---
    assert {:error, :fenced_member_epoch} =
             API.consumer_group_heartbeat(client, group, member_id: a.id, member_epoch: a.epoch + 10)

    # after being fenced the member rejoins with epoch 0 and its own member id
    assert {:ok, %ConsumerGroupHeartbeat{member_id: rejoined_id, member_epoch: epoch}} =
             API.consumer_group_heartbeat(client, group,
               member_id: a.id,
               member_epoch: 0,
               subscribed_topic_names: [topic],
               topic_partitions: [],
               rebalance_timeout_ms: 30_000
             )

    assert rejoined_id == a.id
    assert epoch > 0

    assert {:ok, %ConsumerGroupHeartbeat{member_epoch: -1}} =
             API.consumer_group_heartbeat(client, group, member_id: a.id, member_epoch: -1)
  end

  test "a join without rebalance_timeout_ms is rejected by the broker", %{client: client} do
    topic = create_topic!(client, 1)

    assert {:error, :invalid_request} =
             API.consumer_group_heartbeat(client, "kafka4_cg_" <> generate_random_string(),
               member_epoch: 0,
               subscribed_topic_names: [topic],
               topic_partitions: []
             )
  end

  test "describing an unknown group returns :group_id_not_found", %{client: client} do
    assert {:error, :group_id_not_found} =
             API.describe_consumer_group(client, "kafka4_no_such_" <> generate_random_string())
  end

  test "an unknown server assignor is rejected", %{client: client} do
    topic = create_topic!(client, 1)

    assert {:error, :unsupported_assignor} =
             API.consumer_group_heartbeat(client, "kafka4_cg_" <> generate_random_string(),
               member_epoch: 0,
               subscribed_topic_names: [topic],
               topic_partitions: [],
               rebalance_timeout_ms: 30_000,
               server_assignor: "no-such-assignor"
             )
  end

  test "regex subscriptions (v1) receive matching topics", %{client: client} do
    topic = create_topic!(client, 2)
    group = "kafka4_cg_" <> generate_random_string()

    {:ok, joined} =
      API.consumer_group_heartbeat(client, group,
        member_epoch: 0,
        subscribed_topic_regex: Regex.escape(topic),
        topic_partitions: [],
        rebalance_timeout_ms: 30_000
      )

    member = adopt(%{id: joined.member_id, epoch: 0, owned: []}, joined)
    [member] = converge(client, group, [member], fn [m] -> partitions_of(m) == [0, 1] end)
    assert [%TopicPartitions{topic_name: ^topic}] = member.owned

    {:ok, %ConsumerGroupDescription{members: [described]}} = API.describe_consumer_group(client, group)
    assert described.subscribed_topic_regex == Regex.escape(topic)

    {:ok, _} = API.consumer_group_heartbeat(client, group, member_id: member.id, member_epoch: -1)
  end

  describe "per-partition GenConsumers with a KIP-848 member epoch" do
    alias KafkaEx.Consumer.GenConsumer
    alias KafkaEx.Consumer.GenConsumer.Supervisor, as: GenConsumerSupervisor

    defp start_consumer_sup(client, group) do
      {:ok, sup} =
        GenConsumerSupervisor.start_link(%{
          gen_consumer_module: GenConsumer,
          consumer_module: ForwardingConsumer,
          group_name: group,
          assignments: [],
          opts: [
            client: client,
            extra_consumer_args: %{test_pid: self()},
            auto_offset_reset: :earliest,
            fetch_options: [max_wait_time: 500]
          ]
        })

      Process.unlink(sup)
      on_exit(fn -> KafkaEx.TestSupport.ProcessHelpers.stop_safely(sup) end)
      sup
    end

    defp await_consumed(pid, offset) do
      receive do
        {:consumed, ^pid, offsets} -> if offset in offsets, do: :ok, else: await_consumed(pid, offset)
      after
        15_000 -> flunk("#{inspect(pid)} did not consume offset #{offset}")
      end
    end

    defp committed(client, group, topic, member) do
      {:ok, [%{partition_offsets: [%{offset: offset}]}]} =
        API.fetch_committed_offset(client, group, topic, [%{partition_num: 0}],
          member_id: member.id,
          member_epoch: member.epoch
        )

      offset
    end

    test "consumes and commits with its epoch, follows an epoch bump, and is not restarted when killed",
         %{client: client} do
      topic = create_topic!(client, 1)
      group = "kafka4_cg_" <> generate_random_string()
      {:ok, _} = API.produce(client, topic, 0, [%{value: "a"}])

      a = join!(client, group, topic)
      [a] = converge(client, group, [a], fn [a] -> partitions_of(a) == [0] end)
      start_epoch = a.epoch

      sup = start_consumer_sup(client, group)
      {:ok, pid} = GenConsumerSupervisor.start_partition(sup, {topic, 0}, member_id: a.id, member_epoch: start_epoch)
      await_consumed(pid, 0)
      wait_until(fn -> committed(client, group, topic, a) == 1 end)

      # A second member joins: a keeps p0 and moves to the new epoch.
      b = join!(client, group, topic)
      [a, _b] = converge(client, group, [a, b], fn [a, b] -> a.epoch > start_epoch and a.epoch == b.epoch end)
      assert partitions_of(a) == [0]
      assert :ok = GenConsumer.update_membership(pid, member_epoch: a.epoch)

      {:ok, _} = API.produce(client, topic, 0, [%{value: "b"}])
      await_consumed(pid, 1)
      wait_until(fn -> committed(client, group, topic, a) == 2 end)
      assert %{member_epoch: epoch} = :sys.get_state(pid)
      assert epoch == a.epoch

      # :temporary: nothing restarts it behind the owner.
      ref = Process.monitor(pid)
      Process.exit(pid, :kill)
      assert_receive {:DOWN, ^ref, :process, ^pid, :killed}
      Process.sleep(200)
      assert GenConsumerSupervisor.lookup_partition(sup, {topic, 0}) == :error
      assert DynamicSupervisor.which_children(sup) == []

      # The owner restarts it with the current epoch; it resumes after the
      # committed offset.
      {:ok, new_pid} = GenConsumerSupervisor.start_partition(sup, {topic, 0}, member_id: a.id, member_epoch: a.epoch)
      {:ok, _} = API.produce(client, topic, 0, [%{value: "c"}])
      await_consumed(new_pid, 2)
      refute_received {:consumed, ^new_pid, [0 | _]}
    end

    test "a startup OffsetFetch from an unknown member tells the owner and stops cleanly", %{client: client} do
      topic = create_topic!(client, 1)
      group = "kafka4_cg_" <> generate_random_string()
      a = join!(client, group, topic)
      [a] = converge(client, group, [a], fn [a] -> partitions_of(a) == [0] end)
      {:ok, manager} = FakeManager.start_link(self())

      sup = start_consumer_sup(client, group)

      {:ok, pid} =
        GenConsumerSupervisor.start_partition(sup, {topic, 0},
          member_id: "not-a-member",
          member_epoch: a.epoch,
          group_manager_pid: manager
        )

      ref = Process.monitor(pid)
      assert_receive {:DOWN, ^ref, :process, ^pid, {:shutdown, {:rejoin_required, :unknown_member_id}}}, 15_000
      assert_receive {:manager_received, {:rejoin_required, :unknown_member_id, epoch}}
      assert epoch == a.epoch
      assert Process.alive?(sup)
    end

    test "a startup OffsetFetch with a stale epoch waits for update_membership/3", %{client: client} do
      topic = create_topic!(client, 1)
      group = "kafka4_cg_" <> generate_random_string()
      {:ok, _} = API.produce(client, topic, 0, [%{value: "a"}])
      a = join!(client, group, topic)
      [a] = converge(client, group, [a], fn [a] -> partitions_of(a) == [0] end)

      sup = start_consumer_sup(client, group)

      {:ok, pid} =
        GenConsumerSupervisor.start_partition(sup, {topic, 0}, member_id: a.id, member_epoch: a.epoch + 1)

      refute_receive {:consumed, ^pid, _}, 1_000
      assert Process.alive?(pid)
      assert :ok = GenConsumer.update_membership(pid, member_epoch: a.epoch)
      await_consumed(pid, 0)
    end

    test "OffsetCommit pinned below v9 stops the consumer as terminal", %{client: client} do
      Process.flag(:trap_exit, true)
      topic = create_topic!(client, 1)
      group = "kafka4_cg_" <> generate_random_string()
      {:ok, _} = API.produce(client, topic, 0, [%{value: "a"}])
      a = join!(client, group, topic)
      [a] = converge(client, group, [a], fn [a] -> partitions_of(a) == [0] end)

      {:ok, pid} =
        GenConsumer.start_link(ForwardingConsumer, group, topic, 0,
          client: client,
          member_id: a.id,
          member_epoch: a.epoch,
          auto_offset_reset: :earliest,
          api_versions: %{offset_commit: 8},
          extra_consumer_args: %{test_pid: self()}
        )

      assert_receive {:EXIT, ^pid, {:shutdown, {:terminal_error, :invalid_api_version}}}, 15_000
      assert Process.alive?(client)
    end
  end
end
