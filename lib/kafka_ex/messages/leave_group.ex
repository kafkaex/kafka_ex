defmodule KafkaEx.Messages.LeaveGroup do
  @moduledoc """
  This module represents LeaveGroup response from Kafka.

  ## Versions

  - **V0**: Returns `{:ok, :no_error}` on success (no struct).
  - **V1-V2**: Returns `%LeaveGroup{throttle_time_ms: ...}`.
  - **V3+**: Returns `%LeaveGroup{throttle_time_ms: ..., members: [...]}` with per-member
    leave results including individual error codes (KIP-345 batch leave).
  """

  @type member_result :: %{
          member_id: String.t(),
          group_instance_id: String.t() | nil,
          error: atom()
        }

  defstruct [:throttle_time_ms, :members]

  @type t :: %__MODULE__{
          throttle_time_ms: nil | non_neg_integer(),
          members: nil | [member_result()]
        }

  @doc """
  Builds a LeaveGroup struct from response data.
  """
  @spec build(Keyword.t()) :: t()
  def build(opts \\ []) do
    %__MODULE__{
      throttle_time_ms: Keyword.get(opts, :throttle_time_ms),
      members: Keyword.get(opts, :members)
    }
  end
end
