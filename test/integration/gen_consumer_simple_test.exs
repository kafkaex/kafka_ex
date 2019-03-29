defmodule KafkaEx.GenConsumerSimple.Test do
  use ExUnit.Case
  alias KafkaEx.GenConsumer
  alias KafkaEx.Protocol.OffsetFetch.Request, as: OffsetFetchRequest

  import TestHelper

  require Logger

  @moduletag :integration

  @topic_name "gen_consumer_simple_test"
  @consumer_group_name "gen_consumer_simple_test_group"

  defmodule TestConsumer do
    use KafkaEx.GenConsumer
    alias KafkaEx.GenConsumer

    def message_sets(pid) do
      GenConsumer.call(pid, :message_sets)
    end

    def init(_topic, _partition) do
      {:ok, %{message_sets: []}}
    end

    def handle_call(:message_sets, _from, state) do
      {:reply, state.message_sets, state}
    end

    def handle_message_set(message_set, state) do
      Logger.debug(fn ->
        "Consumer #{inspect(self())} handled message set #{inspect(message_set)}"
      end)

      {
        :no_commit,
        %{state | message_sets: state.message_sets ++ [message_set]}
      }
    end
  end

  test "consume data with no_commit strategy" do
    {:ok, pid} =
      GenConsumer.start_link(
        TestConsumer,
        @consumer_group_name,
        @topic_name,
        0,
        auto_offset_reset: :latest,
        fetch_options: [offset: -1]
      )

    assert [] == TestConsumer.message_sets(pid)
    KafkaEx.produce(@topic_name, 0, "1")

    wait_for(fn ->
      1 == TestConsumer.message_sets(pid) |> length
    end)

    assert [[%{value: "1"}]] = TestConsumer.message_sets(pid)
    KafkaEx.produce(@topic_name, 0, "2")

    wait_for(fn ->
      2 == TestConsumer.message_sets(pid) |> length
    end)

    assert [[%{value: "1"}], [%{value: "2"}]] = TestConsumer.message_sets(pid)

    [%{partitions: [%{error_code: :unknown_topic_or_partition}]}] =
      KafkaEx.offset_fetch(:kafka_ex, %OffsetFetchRequest{
        consumer_group: @consumer_group_name,
        topic: @topic_name,
        partition: 0
      })
  end
end
