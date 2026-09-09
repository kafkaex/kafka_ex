defmodule KafkaEx.Cluster.PartitionInfo do
  @moduledoc """
  Information about a topic partition.

  Contains metadata about a specific partition including its leader broker,
  replica assignments, and in-sync replica (ISR) list.

  Java equivalent: `org.apache.kafka.common.PartitionInfo`
  """

  alias Kayrock.ErrorCode

  # Informational only: node selection keys off `leader` (< 0 = no leader), not this field.
  defstruct partition_id: nil, leader: -1, replicas: [], isr: [], error_code: :no_error

  @type t :: %__MODULE__{
          partition_id: integer(),
          leader: integer(),
          replicas: [integer()],
          isr: [integer()],
          error_code: atom()
        }

  @doc """
  Builds a PartitionInfo struct from partition metadata response.

  ## Parameters

  The metadata map should contain:
    - `:error_code` - The broker's partition error code (0 = no error), retained as-is. Unlike the
      production `parse_partitions/1`, this constructor applies no error-code whitelist
    - `:partition` - The partition number
    - `:leader` - The leader node ID
    - `:replicas` - List of replica node IDs
    - `:isr` - List of in-sync replica node IDs

  """
  @spec from_partition_metadata(map()) :: t()
  def from_partition_metadata(%{
        error_code: error_code,
        partition_index: partition,
        leader_id: leader,
        replica_nodes: replicas,
        isr_nodes: isr
      }) do
    %__MODULE__{
      partition_id: partition,
      leader: leader,
      replicas: replicas,
      isr: isr,
      error_code: ErrorCode.code_to_atom(error_code)
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
