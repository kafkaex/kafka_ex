defmodule KafkaEx.Protocol.Kayrock.Produce.RequestHelpers do
  @moduledoc """
  Shared helper functions for building Produce requests across all versions.

  Handles the complexity of different message formats:
  - V0-V2: MessageSet format (legacy)
  - V3+: RecordBatch format (modern with headers support)
  """

  alias Kayrock.MessageSet
  alias Kayrock.MessageSet.Message
  alias Kayrock.RecordBatch
  alias Kayrock.RecordBatch.Record
  alias Kayrock.RecordBatch.RecordHeader

  @doc """
  Extracts common fields from request options.
  """
  @spec extract_common_fields(Keyword.t()) :: %{
          topic: String.t(),
          partition: non_neg_integer(),
          messages: [map()],
          acks: integer(),
          timeout: non_neg_integer(),
          compression: atom()
        }
  def extract_common_fields(opts) do
    %{
      topic: Keyword.fetch!(opts, :topic),
      partition: Keyword.fetch!(opts, :partition),
      messages: Keyword.fetch!(opts, :messages),
      acks: Keyword.get(opts, :acks, -1),
      timeout: Keyword.get(opts, :timeout, 5000),
      compression: Keyword.get(opts, :compression, :none)
    }
  end

  @doc """
  Builds a Produce request for V0-V2 using MessageSet format.

  MessageSet format is used for Kafka < 0.11 and doesn't support headers.
  """
  @spec build_request_v0_v2(map(), Keyword.t()) :: map()
  def build_request_v0_v2(request_template, opts) do
    fields = extract_common_fields(opts)
    message_set = build_message_set(fields.messages, fields.compression)

    request_template
    |> Map.put(:acks, fields.acks)
    |> Map.put(:timeout, fields.timeout)
    |> Map.put(:topic_data, [
      %{topic: fields.topic, data: [%{partition: fields.partition, record_set: message_set}]}
    ])
  end

  @doc """
  Builds a Produce request for V3+ using RecordBatch format.

  RecordBatch format supports headers, per-record timestamps, and transactional producers.
  """
  @spec build_request_v3_plus(map(), Keyword.t()) :: map()
  def build_request_v3_plus(request_template, opts) do
    fields = extract_common_fields(opts)
    transactional_id = Keyword.get(opts, :transactional_id)
    record_batch = build_record_batch(fields.messages, fields.compression)

    request_template
    |> Map.put(:transactional_id, transactional_id)
    |> Map.put(:acks, fields.acks)
    |> Map.put(:timeout, fields.timeout)
    |> Map.put(:topic_data, [
      %{
        topic: fields.topic,
        data: [%{partition: fields.partition, record_set: record_batch}]
      }
    ])
  end

  @doc """
  Builds a MessageSet from a list of messages (V0-V2 format).

  Each message in the list should be a map with:
  - `:value` (required) - The message value
  - `:key` (optional) - The message key

  Note: Timestamps and headers are NOT supported in MessageSet format.
  """
  @spec build_message_set([map()], atom()) :: MessageSet.t()
  def build_message_set(messages, compression) do
    %MessageSet{
      messages:
        Enum.map(messages, fn msg ->
          %Message{
            key: Map.get(msg, :key),
            value: Map.get(msg, :value),
            compression: compression
          }
        end)
    }
  end

  @doc """
  Builds a RecordBatch from a list of messages (V3+ format).

  Each message in the list should be a map with:
  - `:value` (required) - The message value
  - `:key` (optional) - The message key
  - `:timestamp` (optional) - Message timestamp in milliseconds
  - `:headers` (optional) - List of {key, value} header tuples
  """
  @spec build_record_batch([map()], atom()) :: RecordBatch.t()
  def build_record_batch(messages, compression) do
    %RecordBatch{
      attributes: compression_to_attributes(compression),
      records:
        Enum.map(messages, fn msg ->
          %Record{
            key: Map.get(msg, :key),
            value: Map.get(msg, :value),
            timestamp: Map.get(msg, :timestamp, -1),
            headers: build_record_headers(Map.get(msg, :headers))
          }
        end)
    }
  end

  @doc """
  Converts compression type atom to RecordBatch attributes byte.
  """
  @spec compression_to_attributes(atom()) :: non_neg_integer()
  def compression_to_attributes(:none), do: 0
  def compression_to_attributes(:gzip), do: 1
  def compression_to_attributes(:snappy), do: 2
  def compression_to_attributes(:lz4), do: 3
  def compression_to_attributes(:zstd), do: 4

  @doc """
  Builds RecordHeader structs from a list of {key, value} tuples.
  """
  @spec build_record_headers(nil | [{String.t(), binary()}]) :: [RecordHeader.t()]
  def build_record_headers(nil), do: []

  def build_record_headers(headers) when is_list(headers) do
    Enum.map(headers, &%RecordHeader{key: elem(&1, 0), value: elem(&1, 1)})
  end
end
