defmodule KafkaEx.KayrockCompatibilityConsumerGroupTest do
  @moduledoc """
  These are tests using the original KafkaEx API with the kayrock server

  These come from consumer_group_test.exs.  Note that even though Kayrock does
  not support this version of Kafka, the original implementations often delegate
  to previous versions. So unless the test is testing functionality that doesn't
  make sense in newer versions of kafka, we will test it here.
  """

  use ExUnit.Case

  @moduletag :new_client

  alias KafkaEx.Protocol, as: Proto

  alias KafkaEx.New.KafkaExAPI

  setup do
    {:ok, pid} =
      KafkaEx.start_link_worker(:no_name, server_impl: KafkaEx.New.Client)

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
      KafkaEx.produce(
        %Proto.Produce.Request{
          topic: random_string,
          partition: 0,
          required_acks: 1,
          messages: [%Proto.Produce.Message{value: "hey"}]
        },
        worker_name: client
      )
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

  test "fetch does not commit offset with auto_commit is set to false", %{
    client: client
  } do
    topic = TestHelper.generate_random_string()

    Enum.each(1..10, fn _ ->
      KafkaEx.produce(
        %Proto.Produce.Request{
          topic: topic,
          partition: 0,
          required_acks: 1,
          messages: [%Proto.Produce.Message{value: "hey"}]
        },
        worker_name: client
      )
    end)

    offset =
      KafkaEx.fetch(
        topic,
        0,
        offset: 0,
        worker_name: client,
        auto_commit: false
      )
      |> hd
      |> Map.get(:partitions)
      |> hd
      |> Map.get(:message_set)
      |> Enum.reverse()
      |> hd
      |> Map.get(:offset)

    offset_fetch_response =
      KafkaEx.offset_fetch(client, %Proto.OffsetFetch.Request{
        topic: topic,
        partition: 0
      })
      |> hd

    offset_fetch_response_offset =
      offset_fetch_response.partitions |> hd |> Map.get(:offset)

    refute offset == offset_fetch_response_offset
  end

  test "offset_fetch does not override consumer_group", %{client: client} do
    topic = TestHelper.generate_random_string()
    consumer_group = "bar#{topic}"

    KafkaExAPI.set_consumer_group_for_auto_commit(client, consumer_group)

    Enum.each(1..10, fn _ ->
      KafkaEx.produce(
        %Proto.Produce.Request{
          topic: topic,
          required_acks: 1,
          partition: 0,
          messages: [%Proto.Produce.Message{value: "hey"}]
        },
        worker_name: client
      )
    end)

    KafkaEx.offset_fetch(client, %KafkaEx.Protocol.OffsetFetch.Request{
      topic: topic,
      partition: 0
    })

    assert :sys.get_state(client).consumer_group_for_auto_commit ==
             consumer_group
  end

  test "offset_commit commits an offset and offset_fetch retrieves the committed offset",
       %{client: client} do
    random_string = TestHelper.generate_random_string()

    Enum.each(1..10, fn _ ->
      KafkaEx.produce(
        %Proto.Produce.Request{
          topic: random_string,
          partition: 0,
          required_acks: 1,
          messages: [%Proto.Produce.Message{value: "hey"}]
        },
        worker_name: client
      )
    end)

    assert KafkaEx.offset_commit(
             client,
             %Proto.OffsetCommit.Request{
               topic: random_string,
               offset: 9,
               partition: 0
             }
           ) ==
             [
               %Proto.OffsetCommit.Response{
                 partitions: [%{error_code: :no_error, partition: 0}],
                 topic: random_string
               }
             ]

    assert KafkaEx.offset_fetch(
             client,
             %Proto.OffsetFetch.Request{topic: random_string, partition: 0}
           ) ==
             [
               %Proto.OffsetFetch.Response{
                 partitions: [
                   %{
                     metadata: "",
                     error_code: :no_error,
                     offset: 9,
                     partition: 0
                   }
                 ],
                 topic: random_string
               }
             ]
  end

  test "stream auto_commits offset by default", %{client: client} do
    random_string = TestHelper.generate_random_string()

    KafkaEx.produce(
      %Proto.Produce.Request{
        topic: random_string,
        partition: 0,
        required_acks: 1,
        messages: [
          %Proto.Produce.Message{value: "hey"},
          %Proto.Produce.Message{value: "hi"}
        ]
      },
      worker_name: client
    )

    stream =
      KafkaEx.stream(
        random_string,
        0,
        worker_name: client,
        offset: 0
      )

    log = TestHelper.wait_for_any(fn -> Enum.take(stream, 2) end)

    refute Enum.empty?(log)

    [offset_fetch_response | _] =
      KafkaEx.offset_fetch(client, %Proto.OffsetFetch.Request{
        topic: random_string,
        partition: 0
      })

    error_code = offset_fetch_response.partitions |> hd |> Map.get(:error_code)
    offset = offset_fetch_response.partitions |> hd |> Map.get(:offset)

    assert error_code == :no_error
    refute offset == 0
  end

  test "streams with a consumer group begin at the last committed offset", %{
    client: client
  } do
    topic_name = TestHelper.generate_random_string()
    consumer_group = "stream_test"

    KafkaExAPI.set_consumer_group_for_auto_commit(client, consumer_group)

    messages_in =
      Enum.map(
        1..10,
        fn ix -> %Proto.Produce.Message{value: "Msg #{ix}"} end
      )

    KafkaEx.produce(
      %Proto.Produce.Request{
        topic: topic_name,
        partition: 0,
        required_acks: 1,
        messages: messages_in
      },
      worker_name: client
    )

    stream1 =
      KafkaEx.stream(
        topic_name,
        0,
        worker_name: client,
        no_wait_at_logend: true,
        auto_commit: true,
        offset: 0,
        consumer_group: consumer_group
      )

    assert ["Msg 1", "Msg 2"] ==
             stream1
             |> Enum.take(2)
             |> Enum.map(fn msg -> msg.value end)

    stream2 =
      KafkaEx.stream(
        topic_name,
        0,
        worker_name: client,
        no_wait_at_logend: true,
        auto_commit: true,
        consumer_group: consumer_group
      )

    message_values =
      stream2
      |> Enum.take(2)
      |> Enum.map(fn msg -> msg.value end)

    assert ["Msg 3", "Msg 4"] == message_values
  end
end
