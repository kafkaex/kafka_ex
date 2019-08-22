defmodule KafkaEx.New.Partition do
  @moduledoc """
  Encapsulates what we know about a partition
  """

  defstruct partition_id: nil, leader: -1, replicas: [], isr: []

  @type t :: %__MODULE__{}

  @doc false
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
end
