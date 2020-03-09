defmodule KafkaEx.KayrockCompatibilityStreamingTest do
  @moduledoc """
  Tests for streaming using Kayrock for client implementation
  """

  use ExUnit.Case

  alias KafkaEx.Protocol.OffsetFetch.Request, as: OffsetFetchRequest
  alias KafkaEx.Protocol.OffsetFetch.Response, as: OffsetFetchResponse

  @moduletag :new_client

  setup do
    {:ok, pid} =
      KafkaEx.start_link_worker(:no_name, server_impl: KafkaEx.New.Client)

    {:ok, %{client: pid}}
  end

  test "stream with kafka offset storage and timestamps", %{client: client} do
    topic = "kayrock_stream_test_#{:rand.uniform(2_000_000)}"
    partition = 0
    consumer_group = "streamers"

    {:ok, topic} = TestHelper.ensure_append_timestamp_topic(client, topic)

    KafkaEx.produce(topic, partition, "foo 1", api_version: 3)
    KafkaEx.produce(topic, partition, "foo 2", api_version: 3)
    KafkaEx.produce(topic, partition, "foo 3", api_version: 3)

    stream =
      KafkaEx.stream(topic, partition,
        worker_name: client,
        auto_commit: true,
        no_wait_at_logend: true,
        consumer_group: consumer_group,
        api_versions: %{
          fetch: 3,
          offset_fetch: 3,
          offset_commit: 3
        }
      )

    TestHelper.wait_for(fn ->
      length(Enum.take(stream, 3)) == 3
    end)

    [msg1, msg2, msg3] = Enum.take(stream, 3)

    assert msg1.value == "foo 1"
    assert msg2.value == "foo 2"
    assert msg3.value == "foo 3"
    assert is_integer(msg1.timestamp)
    assert msg1.timestamp > 0
    assert is_integer(msg2.timestamp)
    assert msg2.timestamp > 0
    assert is_integer(msg3.timestamp)
    assert msg3.timestamp > 0

    [
      %OffsetFetchResponse{
        partitions: [
          %{error_code: :no_error, offset: offset}
        ]
      }
    ] =
      KafkaEx.offset_fetch(client, %OffsetFetchRequest{
        topic: topic,
        partition: partition,
        consumer_group: consumer_group,
        api_version: 3
      })

    assert is_integer(offset)
    assert offset > 0
  end

  test "streams doesn't skip first message, with an initially empty log", %{
    client: client
  } do
    topic_name = "kayrock_stream_with_empty_log"
    consumer_group = "streamers_with_empty_log"

    {:ok, topic} = TestHelper.ensure_append_timestamp_topic(client, topic_name)

    {:ok, agent} = Agent.start(fn -> [] end)

    stream =
      KafkaEx.stream(
        topic_name,
        0,
        worker_name: client,
        no_wait_at_logend: false,
        auto_commit: true,
        consumer_group: consumer_group,
        api_versions: %{
          fetch: 3,
          offset_fetch: 3,
          offset_commit: 3
        }
      )

    streamer =
      Task.async(fn ->
        stream
        |> Stream.map(&Agent.update(agent, fn messages -> [&1 | messages] end))
        |> Stream.run()
      end)

    Process.sleep(100)

    KafkaEx.produce(topic, 0, "Msg 1", api_version: 3)
    KafkaEx.produce(topic, 0, "Msg 2", api_version: 3)
    KafkaEx.produce(topic, 0, "Msg 3", api_version: 3)

    Process.sleep(100)

    assert ["Msg 1", "Msg 2", "Msg 3"] ==
             agent
             |> Agent.get(&Enum.reverse/1)
             |> Enum.map(fn message -> message.value end)

    Task.shutdown(streamer)
    Agent.stop(agent)
  end
end
