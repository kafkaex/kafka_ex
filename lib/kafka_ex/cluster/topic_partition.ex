defmodule KafkaEx.Cluster.TopicPartition do
  @moduledoc """
  Represents a specific partition of a Kafka topic.

  A `TopicPartition` is a simple identifier used to uniquely reference a
  partition within a topic. This struct is used extensively across Kafka
  operations for specifying which partition to produce to, consume from,
  or manage offsets for.

  Java equivalent: `org.apache.kafka.common.TopicPartition`
  """

  defstruct [:topic, :partition]

  @type t :: %__MODULE__{
          topic: String.t(),
          partition: non_neg_integer()
        }

  @doc """
  Creates a new TopicPartition struct.

  ## Parameters

    - `topic` - The topic name (must be a non-empty string)
    - `partition` - The partition number (must be a non-negative integer)

  ## Examples

      iex> TopicPartition.new("orders", 0)
      %TopicPartition{topic: "orders", partition: 0}

      iex> TopicPartition.new("events", 3)
      %TopicPartition{topic: "events", partition: 3}

  """
  @spec new(String.t(), non_neg_integer()) :: t()
  def new(topic, partition) when is_binary(topic) and is_integer(partition) and partition >= 0 do
    %__MODULE__{topic: topic, partition: partition}
  end

  @doc """
  Builds a TopicPartition struct from keyword options.

  ## Options

    - `:topic` - (required) The topic name
    - `:partition` - (required) The partition number

  ## Examples

      iex> TopicPartition.build(topic: "orders", partition: 0)
      %TopicPartition{topic: "orders", partition: 0}

  """
  @spec build(Keyword.t()) :: t()
  def build(opts) do
    topic = Keyword.fetch!(opts, :topic)
    partition = Keyword.fetch!(opts, :partition)
    new(topic, partition)
  end

  @doc """
  Converts a TopicPartition to a tuple `{topic, partition}`.

  This is useful for interoperability with APIs that expect tuple format.

  ## Examples

      iex> tp = TopicPartition.new("orders", 0)
      iex> TopicPartition.to_tuple(tp)
      {"orders", 0}

  """
  @spec to_tuple(t()) :: {String.t(), non_neg_integer()}
  def to_tuple(%__MODULE__{topic: topic, partition: partition}) do
    {topic, partition}
  end

  @doc """
  Creates a TopicPartition from a tuple `{topic, partition}`.

  ## Examples

      iex> TopicPartition.from_tuple({"orders", 0})
      %TopicPartition{topic: "orders", partition: 0}

  """
  @spec from_tuple({String.t(), non_neg_integer()}) :: t()
  def from_tuple({topic, partition}) when is_binary(topic) and is_integer(partition) do
    new(topic, partition)
  end
end
