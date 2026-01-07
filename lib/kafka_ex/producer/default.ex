defmodule KafkaEx.Producer.Partitioner.Default do
  @moduledoc """
  Default partitioner implementation.

  When a message key is provided, the partition is determined using a murmur2 hash
  of the key. This ensures that messages with the same key always go to the same
  partition, providing ordering guarantees per-key.

  When no message key is provided, a random partition is selected.

  This implementation is compatible with the Java Kafka client's default partitioner.
  """
  @behaviour KafkaEx.Producer.Partitioner

  alias KafkaEx.Support.Murmur

  @doc """
  Assigns a partition based on the message key.

   - If key is nil, assigns a random partition
   - If key is provided, uses murmur2 hash to consistently assign to the same partition
  """
  @impl KafkaEx.Producer.Partitioner
  @spec assign_partition(String.t(), binary() | nil, binary(), pos_integer()) :: non_neg_integer()
  def assign_partition(_topic, nil, _value, partition_count) do
    :rand.uniform(partition_count) - 1
  end

  def assign_partition(_topic, key, _value, partition_count) when is_binary(key) do
    key |> Murmur.umurmur2() |> rem(partition_count)
  end
end
