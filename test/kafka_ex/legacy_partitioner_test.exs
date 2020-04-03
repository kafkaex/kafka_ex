defmodule KafkaEx.LegacyPartitionerTest do
  alias KafkaEx.LegacyPartitioner

  alias KafkaEx.Protocol.Produce.Request, as: ProduceRequest
  alias KafkaEx.Protocol.Produce.Message, as: ProduceMessage
  alias KafkaEx.Protocol.Metadata.Response, as: MetadataResponse
  alias KafkaEx.Protocol.Metadata.TopicMetadata
  alias KafkaEx.Protocol.Metadata.PartitionMetadata

  import ExUnit.CaptureLog

  use ExUnit.Case

  def metadata(partitions \\ 5) do
    %MetadataResponse{
      topic_metadatas: [
        %TopicMetadata{
          topic: "test_topic",
          partition_metadatas:
            Enum.map(0..(partitions - 1), fn n ->
              %PartitionMetadata{
                partition_id: n
              }
            end)
        }
      ]
    }
  end

  test "no assignment" do
    request = %ProduceRequest{
      topic: "test_topic",
      partition: 2,
      messages: [
        %ProduceMessage{key: nil, value: "message"}
      ]
    }

    %{partition: 2} = LegacyPartitioner.assign_partition(request, metadata(5))
  end

  test "random assignment" do
    request = %ProduceRequest{
      topic: "test_topic",
      partition: nil,
      messages: [
        %ProduceMessage{key: nil, value: "message"}
      ]
    }

    %{partition: partition} =
      LegacyPartitioner.assign_partition(request, metadata(5))

    assert partition >= 0 and partition < 5
  end

  test "key based assignment" do
    request = %ProduceRequest{
      topic: "test_topic",
      partition: nil,
      messages: [
        %ProduceMessage{key: "key", value: "message"}
      ]
    }

    %{partition: 4} = LegacyPartitioner.assign_partition(request, metadata(5))
    %{partition: 3} = LegacyPartitioner.assign_partition(request, metadata(6))

    second_request = %ProduceRequest{
      topic: "test_topic",
      partition: nil,
      messages: [
        %ProduceMessage{key: "key2", value: "message"}
      ]
    }

    %{partition: 1} =
      LegacyPartitioner.assign_partition(second_request, metadata(5))

    %{partition: 5} =
      LegacyPartitioner.assign_partition(second_request, metadata(6))
  end

  test "produce request with inconsistent keys" do
    request = %ProduceRequest{
      topic: "test_topic",
      partition: nil,
      messages: [
        %ProduceMessage{key: "key-1", value: "message-1"},
        %ProduceMessage{key: "key-2", value: "message-2"}
      ]
    }

    assert capture_log(fn ->
             LegacyPartitioner.assign_partition(request, metadata(5))
           end) =~
             "KafkaEx.LegacyPartitioner: couldn't assign partition due to :inconsistent_keys"
  end
end
