defmodule KafkaEx.Messages.Fetch.Record do
  @moduledoc """
  Represents a single record fetched from Kafka.

  This struct aligns with Java Kafka client's `ConsumerRecord` class.
  It contains the record payload along with metadata like offset, key,
  timestamp, and headers.

  Field names align with Java's `ConsumerRecord`:
  - `topic` - the topic name
  - `partition` - the partition number
  - `offset` - position in the Kafka partition
  - `key` - the record key (nullable)
  - `value` - the record content (nullable)
  - `timestamp` - the record timestamp
  - `timestamp_type` - :create_time or :log_append_time
  - `headers` - record headers (list of `Header` structs)
  - `serialized_key_size` - size of serialized key in bytes (-1 if nil)
  - `serialized_value_size` - size of serialized value in bytes (-1 if nil)
  - `leader_epoch` - the leader epoch (optional, for newer formats)
  """

  alias KafkaEx.Cluster.TopicPartition
  alias KafkaEx.Messages.Header

  @type timestamp_type :: :create_time | :log_append_time | nil

  defstruct [
    :topic,
    :partition,
    :offset,
    :key,
    :value,
    :timestamp,
    :timestamp_type,
    :headers,
    :serialized_key_size,
    :serialized_value_size,
    :leader_epoch,
    # Legacy fields from wire format
    :attributes,
    :crc
  ]

  @type t :: %__MODULE__{
          topic: String.t() | nil,
          partition: non_neg_integer() | nil,
          offset: non_neg_integer(),
          key: binary() | nil,
          value: binary() | nil,
          timestamp: integer() | nil,
          timestamp_type: timestamp_type(),
          headers: [Header.t()] | nil,
          serialized_key_size: integer() | nil,
          serialized_value_size: integer() | nil,
          leader_epoch: non_neg_integer() | nil,
          attributes: non_neg_integer() | nil,
          crc: non_neg_integer() | nil
        }

  @doc """
  Builds a Record struct from response data.
  """
  @spec build(Keyword.t()) :: t()
  def build(opts) do
    key = Keyword.get(opts, :key)
    value = Keyword.get(opts, :value)

    %__MODULE__{
      topic: Keyword.get(opts, :topic),
      partition: Keyword.get(opts, :partition),
      offset: Keyword.fetch!(opts, :offset),
      key: key,
      value: value,
      timestamp: Keyword.get(opts, :timestamp),
      timestamp_type: Keyword.get(opts, :timestamp_type),
      headers: Keyword.get(opts, :headers),
      serialized_key_size: Keyword.get(opts, :serialized_key_size, compute_size(key)),
      serialized_value_size: Keyword.get(opts, :serialized_value_size, compute_size(value)),
      leader_epoch: Keyword.get(opts, :leader_epoch),
      attributes: Keyword.get(opts, :attributes),
      crc: Keyword.get(opts, :crc)
    }
  end

  @doc """
  Returns true if the record has a non-nil value.
  """
  @spec has_value?(t()) :: boolean()
  def has_value?(%__MODULE__{value: nil}), do: false
  def has_value?(%__MODULE__{}), do: true

  @doc """
  Returns true if the record has a non-nil key.
  """
  @spec has_key?(t()) :: boolean()
  def has_key?(%__MODULE__{key: nil}), do: false
  def has_key?(%__MODULE__{}), do: true

  @doc """
  Returns true if the record has headers.
  """
  @spec has_headers?(t()) :: boolean()
  def has_headers?(%__MODULE__{headers: nil}), do: false
  def has_headers?(%__MODULE__{headers: []}), do: false
  def has_headers?(%__MODULE__{}), do: true

  @doc """
  Gets a header value by key. Returns nil if not found.
  """
  @spec get_header(t(), binary()) :: binary() | nil
  def get_header(%__MODULE__{headers: nil}, _key), do: nil

  def get_header(%__MODULE__{headers: headers}, key) do
    case Enum.find(headers, fn h -> h.key == key end) do
      %Header{value: value} -> value
      nil -> nil
    end
  end

  @doc """
  Returns a TopicPartition struct for this record.
  """
  @spec topic_partition(t()) :: TopicPartition.t()
  def topic_partition(%__MODULE__{topic: topic, partition: partition}) do
    TopicPartition.new(topic, partition)
  end

  # Private helpers

  defp compute_size(nil), do: -1
  defp compute_size(binary) when is_binary(binary), do: byte_size(binary)
  defp compute_size(_), do: -1
end
