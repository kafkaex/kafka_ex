defmodule KafkaEx.Kafka4.GenConsumerMaxRecordsTest do
  @moduledoc """
  `:max_records` against a Kafka 4.4.0 broker, through a `ConsumerGroup`
  (classic and KIP-848 consumer protocol): 25 records produced before the
  group starts arrive in one fetch and are handed to `handle_message_set/2`
  in chunks of at most 4, each consumed exactly once, in order, and the
  committed offset ends at the end of the partition.
  """
  use ExUnit.Case, async: false

  @moduletag :kafka4
  @moduletag timeout: 300_000

  import KafkaEx.TestHelpers, only: [generate_random_string: 0]

  alias KafkaEx.API
  alias KafkaEx.Cluster.ClusterMetadata
  alias KafkaEx.Cluster.Topic
  alias KafkaEx.Consumer.ConsumerGroup
  alias KafkaEx.Kafka4TestHelpers

  @records 25
  @max_records 4

  defmodule ChunkConsumer do
    @moduledoc false
    use KafkaEx.Consumer.GenConsumer

    def init(_topic, _partition, %{test_pid: test_pid}), do: {:ok, test_pid}

    def handle_message_set(records, test_pid) do
      send(test_pid, {:chunk, Enum.map(records, & &1.offset), Enum.map(records, & &1.value)})
      {:sync_commit, test_pid}
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

  for protocol <- [:classic, :consumer] do
    test "group_protocol: #{protocol}: chunks of at most max_records, each record once, committed to the end", ctx do
      protocol = unquote(protocol)
      topic = "kafka4_maxrec_" <> generate_random_string()
      group = "kafka4_maxrec_" <> generate_random_string()
      {:ok, _} = API.create_topic(ctx.client, topic, num_partitions: 1, replication_factor: 1)
      wait_until(fn -> leader_known?(ctx.client, topic) end)

      values = for i <- 0..(@records - 1), do: "r#{i}"
      {:ok, _} = API.produce(ctx.client, topic, 0, Enum.map(values, &%{value: &1}))

      protocol_opts =
        case protocol do
          :classic -> [group_protocol: :classic]
          :consumer -> [group_protocol: :consumer, leave_timeout_ms: 5_000]
        end

      opts =
        Kafka4TestHelpers.consumer_group_opts(
          ctx,
          [
            extra_consumer_args: %{test_pid: self()},
            max_records: @max_records,
            fetch_options: [max_wait_time: 500]
          ] ++ protocol_opts
        )

      opts = if protocol == :consumer, do: Keyword.drop(opts, [:heartbeat_interval, :session_timeout]), else: opts

      {:ok, sup} = ConsumerGroup.start_link(ChunkConsumer, group, [topic], opts)
      Process.unlink(sup)

      chunks = collect_chunks(@records - 1, [])
      delivered = Enum.flat_map(chunks, &elem(&1, 0))

      # Every chunk is at most max_records, in offset order, values matching.
      for {offsets, chunk_values} <- chunks do
        assert length(offsets) in 1..@max_records
        assert offsets == Enum.sort(offsets) and offsets == Enum.to_list(hd(offsets)..List.last(offsets))
        assert chunk_values == Enum.map(offsets, &Enum.at(values, &1))
      end

      # Every record is delivered. A rebalance mid-run legitimately replays
      # uncommitted records (at-least-once); without one, each record arrives
      # exactly once: one produce = one record batch = one fetch, split
      # 4+4+4+4+4+4+1.
      assert MapSet.new(delivered) == MapSet.new(0..(@records - 1))

      if delivered == Enum.to_list(0..(@records - 1)) do
        assert Enum.map(chunks, fn {offsets, _} -> length(offsets) end) == [4, 4, 4, 4, 4, 4, 1]
      end

      # :sync_commit per chunk: the last chunk's commit is the end of the log.
      wait_until(fn -> committed(ctx.client, group, topic) == @records end)

      Supervisor.stop(sup, :shutdown, 60_000)
      assert committed(ctx.client, group, topic) == @records
    end
  end

  # Collects chunks until the one holding `last_offset` (replays included).
  defp collect_chunks(last_offset, acc) do
    assert_receive {:chunk, offsets, values}, 60_000
    acc = [{offsets, values} | acc]
    if last_offset in offsets, do: Enum.reverse(acc), else: collect_chunks(last_offset, acc)
  end

  defp leader_known?(client, topic) do
    case API.metadata(client, [topic], []) do
      {:ok, %ClusterMetadata{topics: %{^topic => %Topic{partitions: [p]}}}} -> p.leader >= 0
      _ -> false
    end
  end

  defp committed(client, group, topic) do
    case API.fetch_committed_offset(client, group, topic, [%{partition_num: 0}]) do
      {:ok, [%{partition_offsets: [%{offset: offset}]}]} -> offset
      _ -> nil
    end
  end

  defp wait_until(fun, timeout \\ 60_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_wait_until(fun, deadline)
  end

  defp do_wait_until(fun, deadline) do
    cond do
      fun.() -> :ok
      System.monotonic_time(:millisecond) > deadline -> flunk("condition not met in time")
      true -> Process.sleep(200) && do_wait_until(fun, deadline)
    end
  end
end
