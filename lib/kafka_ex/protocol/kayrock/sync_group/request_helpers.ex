defmodule KafkaEx.Protocol.Kayrock.SyncGroup.RequestHelpers do
  @moduledoc """
  Shared helper functions for building SyncGroup requests across all versions.
  """

  @doc """
  Extracts common fields from request options and transforms group assignments.
  """
  @spec extract_common_fields(Keyword.t()) :: %{
          group_id: String.t(),
          generation_id: integer(),
          member_id: String.t(),
          group_assignment: [map()]
        }
  def extract_common_fields(opts) do
    %{
      group_id: Keyword.fetch!(opts, :group_id),
      generation_id: Keyword.fetch!(opts, :generation_id),
      member_id: Keyword.fetch!(opts, :member_id),
      group_assignment: Keyword.get(opts, :group_assignment, [])
    }
  end

  @doc """
  Builds a SyncGroup request by populating the request template with extracted fields.
  This is shared logic between V0 and V1 requests as they have identical structure.
  """
  @spec build_request_from_template(map(), Keyword.t()) :: map()
  def build_request_from_template(request_template, opts) do
    %{
      group_id: group_id,
      generation_id: generation_id,
      member_id: member_id,
      group_assignment: group_assignment
    } = extract_common_fields(opts)

    request_template
    |> Map.put(:group_id, group_id)
    |> Map.put(:generation_id, generation_id)
    |> Map.put(:member_id, member_id)
    |> Map.put(:group_assignment, group_assignment)
  end
end
