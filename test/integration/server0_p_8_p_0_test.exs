defmodule KafkaEx.Server0P9P0.Test do
  use ExUnit.Case
  import TestHelper

  @moduletag :server_0_p_8_p_0

  alias KafkaEx.Server0P8P0, as: Server

  @topic "test0p8p0"

  test "can produce and fetch a message" do
    {:ok, args} = KafkaEx.build_worker_options([])
    {:ok, worker} = Server.start_link(args, :no_name)

    now = :erlang.monotonic_time
    msg = "test message #{now}"
    partition = 0
    :ok = KafkaEx.produce(@topic, partition, msg, worker_name: worker)

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

    Process.unlink(worker)
    GenServer.stop(worker)
    refute Process.alive?(worker)
  end
end
