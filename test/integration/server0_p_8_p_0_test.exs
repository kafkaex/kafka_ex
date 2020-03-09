defmodule KafkaEx.Server0P8P0.Test do
  use ExUnit.Case
  import TestHelper

  @moduletag :server_0_p_8_p_0

  alias KafkaEx.Server0P8P0, as: Server

  @topic "test0p8p0"

  setup do
    {:ok, args} = KafkaEx.build_worker_options([])
    {:ok, worker} = Server.start_link([{:name, :no_name} | args])

    # we don't want to crash if the worker crashes
    Process.unlink(worker)

    on_exit(fn ->
      if Process.alive?(worker) do
        Process.exit(worker, :normal)
      end
    end)

    {:ok, [worker: worker]}
  end

  test "can produce and fetch a message", %{worker: worker} do
    now = :erlang.monotonic_time()
    msg = "test message #{now}"
    partition = 0
    :ok = KafkaEx.produce(@topic, partition, msg, worker_name: worker)

    wait_for(fn ->
      [got] =
        KafkaEx.fetch(
          @topic,
          partition,
          worker_name: worker,
          offset: 0,
          auto_commit: false
        )

      [got_partition] = got.partitions
      Enum.any?(got_partition.message_set, fn m -> m.value == msg end)
    end)
  end

  test "does not mem leak when message is not a Produce.Message" do
    name = "create_topic_#{:rand.uniform(2_000_000)}"

    assert {:error, "Invalid produce request"} ==
             KafkaEx.produce(%KafkaEx.Protocol.Produce.Request{
               messages: [%{key: "key1", value: "value1"}],
               partition: 0,
               required_acks: 0,
               timeout: 500,
               topic: name
             })
  end

  test "when the partition is not found", %{worker: worker} do
    partition = 42

    assert :topic_not_found ==
             KafkaEx.fetch(
               @topic,
               partition,
               worker_name: worker,
               offset: 0,
               auto_commit: false
             )
  end
end
