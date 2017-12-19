defmodule KafkaEx.Server0P8P2.Test do
  use ExUnit.Case
  import TestHelper

  @moduletag :server_0_p_8_p_2

  alias KafkaEx.Server0P8P2, as: Server

  @topic "test0p8p2"
  @consumer_group "group0p8p0"

  defp publish_message(partition, worker) do
    now = :erlang.monotonic_time
    msg = "test message #{now}"
    :ok = KafkaEx.produce(@topic, partition, msg, worker_name: worker)
    msg
  end

  defp get_offset(partition, worker) do
    latest_consumer_offset_number(
      @topic,
      partition,
      @consumer_group,
      worker
    )
  end

  setup do
    {:ok, args} = KafkaEx.build_worker_options(
      [consumer_group: @consumer_group]
    )
    {:ok, worker} = Server.start_link(args, :no_name)

    # we don't want to crash if the worker crashes
    Process.unlink(worker)

    on_exit fn ->
      if Process.alive?(worker) do
        Process.exit(worker, :normal)
      end
    end

    {:ok, [worker: worker]}
  end

  test "can produce and fetch a message", %{worker: worker}do
    partition = 0
    offset_before = get_offset(partition, worker)

    _ = publish_message(partition, worker)
    msg = publish_message(partition, worker)

    wait_for(fn ->
      [got] = KafkaEx.fetch(
        @topic,
        partition,
        worker_name: worker,
        offset: 1,
        auto_commit: false
      )
      [got_partition] = got.partitions
      Enum.any?(got_partition.message_set, fn(m) -> m.value == msg end)
    end)

    # auto_commit was false
    offset_after = get_offset(partition, worker)
    assert offset_after == offset_before
  end

  test "when the partition is not found", %{worker: worker} do
    partition = 42
    assert :topic_not_found == KafkaEx.fetch(
      @topic,
      partition,
      worker_name: worker,
      offset: 1,
      auto_commit: false
    )
  end

  test "offset auto_commit works", %{worker: worker} do
    partition = 0
    offset_before = get_offset(partition, worker)

    _ = publish_message(partition, worker)
    msg = publish_message(partition, worker)

    wait_for(fn ->
      [got] = KafkaEx.fetch(
        @topic,
        partition,
        worker_name: worker,
        offset: 1,
        auto_commit: true
      )
      [got_partition] = got.partitions
      Enum.any?(got_partition.message_set, fn(m) -> m.value == msg end)
    end)

    offset_after = get_offset(partition, worker)
    assert offset_after > offset_before
  end
end
