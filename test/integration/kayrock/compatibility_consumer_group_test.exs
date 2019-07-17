defmodule KafkaEx.KayrockCompatibilityConsumerGroupTest do
  @moduledoc """
  These are tests using the original KafkaEx API with the kayrock server

  These come from consumer_group_test.exs.  Note that even though Kayrock does
  not support this version of Kafka, the original implementations often delegate
  to previous versions. So unless the test is testing functionality that doesn't
  make sense in newer versions of kafka, we will test it here.
  """

  use ExUnit.Case

  @moduletag :server_kayrock

  alias KafkaEx.Protocol, as: Proto

  alias KafkaEx.ServerKayrock
  alias KafkaEx.New.KafkaExAPI

  setup do
    {:ok, args} = KafkaEx.build_worker_options([])

    {:ok, pid} = ServerKayrock.start_link(args, :no_name)

    {:ok, %{client: pid}}
  end

  test "fetching the consumer group from the default worker", %{client: client} do
    assert Application.get_env(:kafka_ex, :consumer_group) ==
             KafkaEx.consumer_group(client)
  end

  test "fetch auto_commits offset by default", %{client: client} do
    topic = "kafka_ex_consumer_group_test"
    consumer_group = "auto_commit_consumer_group"

    KafkaExAPI.set_consumer_group_for_auto_commit(client, consumer_group)

    {:ok, offset_before} = KafkaExAPI.latest_offset(client, topic, 0)

    Enum.each(1..10, fn _ ->
      msg = %Proto.Produce.Message{value: "hey #{inspect(:os.timestamp())}"}

      KafkaEx.produce(
        %Proto.Produce.Request{
          topic: topic,
          partition: 0,
          required_acks: 1,
          messages: [msg]
        },
        worker_name: client
      )
    end)

    {:ok, offset_after} = KafkaExAPI.latest_offset(client, topic, 0)
    assert offset_after == offset_before + 10

    [logs] =
      KafkaEx.fetch(
        topic,
        0,
        offset: offset_before,
        worker_name: client
      )

    [partition] = logs.partitions
    message_set = partition.message_set
    assert 10 == length(message_set)

    last_message = List.last(message_set)
    offset_of_last_message = last_message.offset

    offset_request = %Proto.OffsetFetch.Request{
      topic: topic,
      partition: 0,
      consumer_group: consumer_group
    }

    [offset_fetch_response] = KafkaEx.offset_fetch(client, offset_request)
    [partition] = offset_fetch_response.partitions
    error_code = partition.error_code
    offset_fetch_response_offset = partition.offset

    assert error_code == :no_error
    assert offset_of_last_message == offset_fetch_response_offset
  end

  test "fetch starts consuming from last committed offset", %{client: client} do
    random_string = TestHelper.generate_random_string()
    consumer_group = "auto_commit_consumer_group"
    KafkaExAPI.set_consumer_group_for_auto_commit(client, consumer_group)

    Enum.each(1..10, fn _ ->
      KafkaEx.produce(%Proto.Produce.Request{
        topic: random_string,
        partition: 0,
        required_acks: 1,
        messages: [%Proto.Produce.Message{value: "hey"}]
      })
    end)

    KafkaEx.offset_commit(
      client,
      %Proto.OffsetCommit.Request{topic: random_string, offset: 3, partition: 0}
    )

    logs =
      KafkaEx.fetch(random_string, 0, worker_name: client)
      |> hd
      |> Map.get(:partitions)
      |> hd
      |> Map.get(:message_set)

    first_message = logs |> hd

    assert first_message.offset == 4
    assert length(logs) == 6
  end
end
