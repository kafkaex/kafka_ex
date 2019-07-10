defmodule KafkaEx.New.Adapter do
  @moduledoc """
  Code that converts old-style KafkaEx request structures to and from Kayrock
  structures

  No new code should rely on this code.  This should only be around to support
  the compatibility mode during transition to the new API.
  """

  alias KafkaEx.Protocol.Offset, as: Offset
  alias KafkaEx.Protocol.Offset.Response, as: OffsetResponse
  alias KafkaEx.Protocol.Produce.Request, as: ProduceRequest

  alias Kayrock.MessageSet
  alias Kayrock.MessageSet.Message

  def list_offsets_request(topic, partition, time) do
    time = Offset.parse_time(time)

    partition_request = %{partition: partition, timestamp: time}

    %Kayrock.ListOffsets.V1.Request{
      replica_id: -1,
      topics: [%{topic: topic, partitions: [partition_request]}]
    }
  end

  def list_offsets_response(api_response) do
    Enum.map(api_response.responses, fn r ->
      %OffsetResponse{
        topic: r.topic,
        partition_offsets:
          Enum.map(r.partition_responses, fn p ->
            %{
              error_code: Kayrock.ErrorCode.code_to_atom(p.error_code),
              offset: [p.offset],
              partition: p.partition
            }
          end)
      }
    end)
  end

  def produce_request(kafka_ex_produce_request) do
    %ProduceRequest{
      topic: topic,
      partition: partition,
      required_acks: required_acks,
      timeout: timeout,
      compression: compression,
      messages: messages
    } = kafka_ex_produce_request

    # TODO should make it optional to convert to record batches?
    #   or maybe it's better to force people to update to the new api?
    message_set = %MessageSet{
      messages:
        Enum.map(
          messages,
          &kafka_ex_message_to_kayrock_message(&1, compression)
        )
    }

    request = %Kayrock.Produce.V0.Request{
      acks: required_acks,
      timeout: timeout,
      topic_data: [
        %{
          topic: topic,
          data: [
            %{partition: partition, record_set: message_set}
          ]
        }
      ]
    }

    {topic, partition, request}
  end

  defp kafka_ex_message_to_kayrock_message(msg, compression) do
    %Message{key: msg.key, value: msg.value, compression: compression}
  end
end
