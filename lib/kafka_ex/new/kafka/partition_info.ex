defmodule KafkaEx.New.Kafka.PartitionInfo do
  @moduledoc """
  Information about a topic partition.

  Contains metadata about a specific partition including its leader broker,
  replica assignments, and in-sync replica (ISR) list.

  Java equivalent: `org.apache.kafka.common.PartitionInfo`
  """

  defstruct partition_id: nil, leader: -1, replicas: [], isr: []

  @type t :: %__MODULE__{
          partition_id: integer(),
          leader: integer(),
          replicas: [integer()],
          isr: [integer()]
        }

  @doc """
  Builds a PartitionInfo struct from partition metadata response.

  ## Parameters

  The metadata map should contain:
    - `:error_code` - Must be 0 for successful parsing
    - `:partition` - The partition number
    - `:leader` - The leader node ID
    - `:replicas` - List of replica node IDs
    - `:isr` - List of in-sync replica node IDs

  """
  @spec from_partition_metadata(map()) :: t()
  def from_partition_metadata(%{
        error_code: 0,
        partition: partition,
        leader: leader,
        replicas: replicas,
        isr: isr
      }) do
    %__MODULE__{
      partition_id: partition,
      leader: leader,
      replicas: replicas,
      isr: isr
    }
  end

  @doc """
  Builds a PartitionInfo struct from keyword options.

  ## Options

    - `:partition_id` - (required) The partition number
    - `:leader` - The leader node ID (default: -1)
    - `:replicas` - List of replica node IDs (default: [])
    - `:isr` - List of in-sync replica node IDs (default: [])

  """
  @spec build(Keyword.t()) :: t()
  def build(opts) do
    %__MODULE__{
      partition_id: Keyword.fetch!(opts, :partition_id),
      leader: Keyword.get(opts, :leader, -1),
      replicas: Keyword.get(opts, :replicas, []),
      isr: Keyword.get(opts, :isr, [])
    }
  end

  @doc """
  Returns the partition number.

  Provided for API compatibility with Java's `PartitionInfo.partition()`.
  """
  @spec partition(t()) :: integer()
  def partition(%__MODULE__{partition_id: id}), do: id

  @doc """
  Returns the leader node ID.

  Provided for API compatibility with Java's `PartitionInfo.leader()`.
  Returns -1 if no leader is available.
  """
  @spec leader(t()) :: integer()
  def leader(%__MODULE__{leader: leader}), do: leader

  @doc """
  Returns the list of replica node IDs.

  Provided for API compatibility with Java's `PartitionInfo.replicas()`.
  """
  @spec replicas(t()) :: [integer()]
  def replicas(%__MODULE__{replicas: replicas}), do: replicas

  @doc """
  Returns the list of in-sync replica node IDs.

  Provided for API compatibility with Java's `PartitionInfo.inSyncReplicas()`.
  """
  @spec in_sync_replicas(t()) :: [integer()]
  def in_sync_replicas(%__MODULE__{isr: isr}), do: isr
end
