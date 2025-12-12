defmodule KafkaEx.New.Kafka.Heartbeat do
  @moduledoc """
  This module represents Heartbeat response from Kafka.
  """

  defstruct [:throttle_time_ms]
  @type t :: %__MODULE__{throttle_time_ms: nil | non_neg_integer()}

  @doc """
  Builds a Heartbeat struct from response data.
  """
  @spec build(Keyword.t()) :: t()
  def build(opts \\ []) do
    %__MODULE__{
      throttle_time_ms: Keyword.get(opts, :throttle_time_ms)
    }
  end
end
