defmodule KafkaEx.Producer.Partitioner do
  @moduledoc """
  Behaviour definition for partitioners that assign partitions for produce requests.

  A partitioner is responsible for determining which partition a message should
  be sent to when no explicit partition is provided.

  ## Implementing a Custom Partitioner

  To implement a custom partitioner, create a module that implements the
  `assign_partition/4` callback:

      defmodule MyApp.RoundRobinPartitioner do
        @behaviour KafkaEx.Producer.Partitioner

        def assign_partition(_topic, _key, _value, partition_count) do
          # Simple round-robin - in practice you'd want state for this
          :rand.uniform(partition_count) - 1
        end
      end

  Then configure it in your application:

      config :kafka_ex, partitioner: MyApp.RoundRobinPartitioner

  ## Built-in Partitioners

    * `KafkaEx.Producer.Partitioner.Default` - Uses murmur2 hash of key for consistent
      partitioning, or random selection when no key is provided.
  """

  @doc """
  Assigns a partition for the given message.

  ## Parameters

    * `topic` - The topic name
    * `key` - The message key (may be nil)
    * `value` - The message value
    * `partition_count` - The number of partitions for the topic

  ## Returns

  The partition number (0-indexed) to send the message to.
  """
  @callback assign_partition(
              topic :: String.t(),
              key :: binary() | nil,
              value :: binary(),
              partition_count :: pos_integer()
            ) :: non_neg_integer()

  @doc """
  Returns the configured partitioner module.

  Defaults to `KafkaEx.Producer.Partitioner.Default` if not configured.
  """
  @spec get_partitioner() :: module()
  def get_partitioner do
    Application.get_env(:kafka_ex, :partitioner, KafkaEx.Producer.Partitioner.Default)
  end
end
