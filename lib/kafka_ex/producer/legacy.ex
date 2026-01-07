defmodule KafkaEx.Producer.Partitioner.Legacy do
  @moduledoc """
  Legacy default partitioner implementation.

  **DEPRECATED**: This partitioner will be removed in KafkaEx v2.0.
  Use `KafkaEx.Producer.Partitioner.Default` instead, which is compatible with
  the Java Kafka client's default partitioner.

  ## Background

  This "legacy partitioner" was the default partitioner for KafkaEx before v1.0.
  It was intended to match the behaviour of the default Java client, however
  there were small differences in the murmur2 hash masking:

  - Legacy: Uses unsigned 32-bit masking (`band(0xFFFFFFFF)`)
  - Default: Uses signed 31-bit masking (`band(0x7FFFFFFF)`) - Java compatible

  This legacy partitioner is provided for backward compatibility with existing
  applications that rely on the previous partitioning behavior. New applications
  should use `KafkaEx.Producer.Partitioner.Default`.

  ## Migration

  If you're using this partitioner, migrate to `KafkaEx.Producer.Partitioner.Default`:

      # Before (deprecated)
      config :kafka_ex, partitioner: KafkaEx.Producer.Partitioner.Legacy

      # After
      config :kafka_ex, partitioner: KafkaEx.Producer.Partitioner.Default

  Note: Changing partitioners may cause messages with the same key to be
  routed to different partitions. Plan your migration accordingly.
  """

  @behaviour KafkaEx.Producer.Partitioner

  alias KafkaEx.Support.Murmur

  @deprecated "Use KafkaEx.Producer.Partitioner.Default instead. This module will be removed in v2.0"
  @impl KafkaEx.Producer.Partitioner
  @spec assign_partition(String.t(), binary() | nil, binary(), pos_integer()) :: non_neg_integer()
  def assign_partition(_topic, nil, _value, partition_count) do
    :rand.uniform(partition_count) - 1
  end

  def assign_partition(_topic, key, _value, partition_count) when is_binary(key) do
    hash = Murmur.umurmur2_legacy(key)
    rem(hash, partition_count)
  end
end
