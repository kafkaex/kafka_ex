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
end
