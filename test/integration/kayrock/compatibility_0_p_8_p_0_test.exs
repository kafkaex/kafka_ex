defmodule KafkaEx.KayrockCompatibility0p8p0Test do
  @moduledoc """
  These are tests using the original KafkaEx API with the kayrock server

  These come from server0_p_8_p_0_test.exs.  Note that even though Kayrock does
  not support this version of Kafka, the original implementations often delegate
  to previous versions. So unless the test is testing functionality that doesn't
  make sense in newer versions of kafka, we will test it here.
  """

  use ExUnit.Case

  @moduletag :new_client

  @topic "test0p8p0"

  alias KafkaEx.New.Client

  setup do
    {:ok, args} = KafkaEx.build_worker_options([])

    {:ok, pid} = New.Client.start_link(args, :no_name)

    {:ok, %{client: pid}}
  end

  test "can produce and fetch a message", %{client: client} do
    now = :erlang.monotonic_time()
    msg = "test message #{now}"
    partition = 0
    :ok = KafkaEx.produce(@topic, partition, msg, worker_name: client)

    TestHelper.wait_for(fn ->
      [got] =
        KafkaEx.fetch(
          @topic,
          partition,
          worker_name: client,
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

  test "when the partition is not found", %{client: client} do
    partition = 42

    assert :topic_not_found ==
             KafkaEx.fetch(
               @topic,
               partition,
               worker_name: client,
               offset: 0,
               auto_commit: false
             )
  end
end
