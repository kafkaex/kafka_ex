defmodule KafkaEx.New.Protocols.Kayrock.SyncGroup.RequestHelpers do
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
end
