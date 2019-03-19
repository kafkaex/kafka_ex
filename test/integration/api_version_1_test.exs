defmodule KafkaEx.ApiVersion1.Test do
  use ExUnit.Case
  import TestHelper

  @moduletag :integration

  test "offset commit and fetch" do
    ## Create a separate topic
    topic = "api_version_one_#{:rand.uniform(2_000_000)}"

    request = %{
      topic:              topic,
      num_partitions:     10,
      replication_factor: 1,
      replica_assignment: [],
      config_entries: [
        %{config_name: "cleanup.policy", config_value: "compact"},
        %{config_name: "min.compaction.lag.ms", config_value: "0"}
      ]
    }

    resp = KafkaEx.create_topics([request], timeout: 2000)
    assert {:no_error, topic} == parse_create_topic_resp(resp)

    resp = KafkaEx.create_topics([request], timeout: 2000)
    assert {:topic_already_exists, topic} == parse_create_topic_resp(resp)

    wait_for(fn ->
      topics = KafkaEx.metadata().topic_metadatas |> Enum.map(& &1.topic)
      assert Enum.member?(topics, topic)
    end)

    ## Create the offset request and set the offset using Version 1
    offset_commit_request = %KafkaEx.Protocol.OffsetCommit.V1.Request{
      consumer_group: "kafka_ex",
      offset:         23,
      topic:          topic,
      partition:      0
    }

    resp = KafkaEx.offset_commit(:kafka_ex, offset_commit_request)
    assert [%KafkaEx.Protocol.OffsetCommit.Response{
               partitions: [0], topic: topic}] == resp

    ## Version 1 committed offsets can only be retrieved using Version 1
    ## OffsetFetch requests.
    offset_fetch_request_v1 = %KafkaEx.Protocol.OffsetFetch.V1.Request{
      topic: topic,
      partition: 0
    }
    resp = KafkaEx.offset_fetch(:kafka_ex, offset_fetch_request_v1)
    assert({:no_error, topic, 23, 0} ==
      parse_offset_fetch_resp(resp))

    offset_fetch_request_v0 = %KafkaEx.Protocol.OffsetFetch.Request{
      topic: topic,
      partition: 0
    }
    resp = KafkaEx.offset_fetch(:kafka_ex, offset_fetch_request_v0)
    assert({:unknown_topic_or_partition, topic, -1, 0} ==
      parse_offset_fetch_resp(resp))
  end

  def parse_offset_fetch_resp(response) do
    [%KafkaEx.Protocol.OffsetFetch.Response{
       partitions: [
        %{
          error_code: error_code,
          metadata:   _metadata,
          offset:     offset,
          partition:  partition,
        }
       ],
       topic: topic_name
    }] = response

    {error_code, topic_name, offset, partition}
  end

  def parse_create_topic_resp(response) do
    %KafkaEx.Protocol.CreateTopics.Response{
      topic_errors: [
        %KafkaEx.Protocol.CreateTopics.TopicError{
          error_code: error_code,
          topic_name: topic_name
        }
      ]
    } = response

    {error_code, topic_name}
  end
end
