defmodule KafkaEx.Messages.ConsumerGroupListing do
  @moduledoc """
  A lightweight listing of a single consumer group in the cluster.

  Java equivalent: `org.apache.kafka.clients.admin.ConsumerGroupListing`

  Returned by the ListGroups API. Carries only the group id and protocol type;
  use `KafkaEx.API.describe_group/2` for full group detail (state, members, ...).
  """

  @type t :: %__MODULE__{
          group_id: binary,
          protocol_type: binary
        }

  defstruct ~w(group_id protocol_type)a

  @doc """
  Builds a ConsumerGroupListing from a ListGroups API response group entry.
  """
  @spec from_list_groups_response(map) :: t()
  def from_list_groups_response(group) do
    %__MODULE__{
      group_id: group.group_id,
      protocol_type: group.protocol_type
    }
  end

  @doc """
  Returns the group ID.
  """
  @spec group_id(t()) :: binary
  def group_id(%__MODULE__{group_id: group_id}), do: group_id

  @doc """
  Returns the protocol type (e.g. `"consumer"`).
  """
  @spec protocol_type(t()) :: binary
  def protocol_type(%__MODULE__{protocol_type: protocol_type}), do: protocol_type
end
