defmodule KafkaEx.Messages.SyncGroup do
  @moduledoc """
  This module represents SyncGroup response from Kafka.

  The response includes partition assignments for the consumer group member.
  """

  alias KafkaEx.Messages.ConsumerGroupDescription.Member.MemberAssignment.PartitionAssignment

  defstruct [:throttle_time_ms, :partition_assignments]

  @type t :: %__MODULE__{
          throttle_time_ms: nil | non_neg_integer(),
          partition_assignments: [PartitionAssignment.t()]
        }

  @doc """
  Builds a SyncGroup struct from response data.
  """
  @spec build(Keyword.t()) :: t()
  def build(opts \\ []) do
    %__MODULE__{
      throttle_time_ms: Keyword.get(opts, :throttle_time_ms),
      partition_assignments: Keyword.get(opts, :partition_assignments, [])
    }
  end
end
