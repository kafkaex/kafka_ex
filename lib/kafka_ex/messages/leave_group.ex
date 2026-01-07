defmodule KafkaEx.Messages.LeaveGroup do
  @moduledoc """
  This module represents LeaveGroup response from Kafka.
  """

  defstruct [:throttle_time_ms]
  @type t :: %__MODULE__{throttle_time_ms: nil | non_neg_integer()}

  @doc """
  Builds a LeaveGroup struct from response data.
  """
  @spec build(Keyword.t()) :: t()
  def build(opts \\ []) do
    %__MODULE__{
      throttle_time_ms: Keyword.get(opts, :throttle_time_ms)
    }
  end
end
