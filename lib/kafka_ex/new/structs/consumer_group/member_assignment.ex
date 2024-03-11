defmodule KafkaEx.New.Structs.ConsumerGroup.Member.MemberAssignment do
  @moduledoc """
  Encapsulates what we know about a consumer group.
  The current assignment provided by the group leader (will only be present if the group is stable).
  """

  alias KafkaEx.New.Structs.ConsumerGroup.Member.MemberAssignment.PartitionAssignment

  @type t :: %__MODULE__{
          version: non_neg_integer,
          user_data: binary,
          partition_assignments: [PartitionAssignment.t()]
        }

  defstruct ~w(version partition_assignments user_data)a

  @type response_partial :: %{
          required(:version) => non_neg_integer,
          required(:partition_assignments) => [
            PartitionAssignment.response_partial()
          ],
          required(:user_data) => binary
        }

  @spec from_describe_group_response(response_partial()) :: __MODULE__.t()
  def from_describe_group_response(response) do
    %__MODULE__{
      version: response.version,
      user_data: response.user_data,
      partition_assignments:
        Enum.map(response.partition_assignments, fn assignment ->
          PartitionAssignment.from_describe_group_response(assignment)
        end)
    }
  end
end
