defmodule KafkaEx.New.Adapter do
  @moduledoc """
  Code that converts old-style KafkaEx request structures to and from Kayrock
  structures

  No new code should rely on this code.  This should only be around to support
  the compatibility mode during transition to the new API.
  """

  alias KafkaEx.Protocol.Metadata.Broker
  alias KafkaEx.Protocol.Metadata.TopicMetadata
  alias KafkaEx.Protocol.Metadata.PartitionMetadata
  alias KafkaEx.Protocol.Metadata.Response, as: MetadataResponse
  alias KafkaEx.Protocol.Offset, as: Offset
  alias KafkaEx.Protocol.Offset.Response, as: OffsetResponse
  alias KafkaEx.Protocol.Produce.Request, as: ProduceRequest
  alias KafkaEx.Protocol.Fetch.Response, as: FetchResponse
  alias KafkaEx.Protocol.Fetch.Message, as: FetchMessage

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

  def produce_response(:ok), do: :ok

  def produce_response(%Kayrock.Produce.V0.Response{
        responses: [
          %{
            partition_responses: [
              %{base_offset: base_offset, error_code: 0}
            ]
          }
        ]
      }) do
    base_offset
  end

  def metadata_response(cluster_metadata) do
    brokers =
      cluster_metadata.brokers
      |> Enum.map(fn {node_id, broker} ->
        kayrock_broker_to_kafka_ex_broker(
          {node_id, broker},
          node_id == cluster_metadata.controller_id
        )
      end)

    topics =
      cluster_metadata.topics
      |> Enum.map(&kayrock_topic_to_kafka_ex_topic_metadata/1)

    %MetadataResponse{
      brokers: brokers,
      topic_metadatas: topics
    }
  end

  def fetch_request(fetch_request) do
    {fetch_request.topic, fetch_request.partition,
     %Kayrock.Fetch.V0.Request{
       max_wait_time: fetch_request.wait_time,
       min_bytes: fetch_request.min_bytes,
       replica_id: -1,
       topics: [
         %{
           topic: fetch_request.topic,
           partitions: [
             %{
               partition: fetch_request.partition,
               fetch_offset: fetch_request.offset,
               max_bytes: fetch_request.max_bytes
             }
           ]
         }
       ]
     }}
  end

  def fetch_response(fetch_response) do
    [topic_response | _] = fetch_response.responses
    [partition_response | _] = topic_response.partition_responses

    {message_set, last_offset} =
      kayrock_message_set_to_kafka_ex(partition_response.record_set)

    [
      %FetchResponse{
        topic: topic_response.topic,
        partitions: [
          %{
            partition: partition_response.partition_header.partition,
            error_code: partition_response.partition_header.error_code,
            hw_mark_offset: partition_response.partition_header.high_watermark,
            message_set: message_set,
            last_offset:
              last_offset || partition_response.partition_header.high_watermark
          }
        ]
      }
    ]
  end

  defp kayrock_message_set_to_kafka_ex(%Kayrock.RecordBatch{} = record_batch) do
    messages =
      Enum.map(record_batch.records, fn record ->
        %FetchMessage{
          attributes: record.attributes,
          crc: nil,
          key: record.key,
          value: record.value,
          offset: record.offset
        }
      end)

    case messages do
      [] ->
        {messages, nil}

      _ ->
        last_offset = Enum.max_by(messages, fn m -> m.offset end)
        {messages, last_offset}
    end
  end

  defp kayrock_message_set_to_kafka_ex(%Kayrock.MessageSet{} = message_set) do
    messages =
      Enum.map(message_set.messages, fn message ->
        %FetchMessage{
          attributes: message.attributes,
          crc: message.crc,
          key: message.key,
          value: message.value,
          offset: message.offset
        }
      end)

    case messages do
      [] ->
        {messages, nil}

      _ ->
        last_offset = Enum.max_by(messages, fn m -> m.offset end)
        {messages, last_offset}
    end
  end

  defp kafka_ex_message_to_kayrock_message(msg, compression) do
    %Message{key: msg.key, value: msg.value, compression: compression}
  end

  defp kayrock_broker_to_kafka_ex_broker({node_id, broker}, is_controller) do
    %Broker{
      node_id: node_id,
      host: broker.host,
      port: broker.port,
      socket: broker.socket,
      is_controller: is_controller
    }
  end

  defp kayrock_topic_to_kafka_ex_topic_metadata({topic_name, topic}) do
    %TopicMetadata{
      topic: topic_name,
      is_internal: topic.is_internal,
      partition_metadatas:
        Enum.map(
          topic.partitions,
          &kayrock_partition_to_kafka_ex_partition_metadata/1
        )
    }
  end

  defp kayrock_partition_to_kafka_ex_partition_metadata(partition) do
    %PartitionMetadata{
      partition_id: partition.partition_id,
      leader: partition.leader,
      replicas: partition.replicas,
      isrs: partition.isr
    }
  end
end
