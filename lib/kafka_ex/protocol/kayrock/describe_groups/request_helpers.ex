defmodule KafkaEx.Protocol.Kayrock.DescribeGroups.RequestHelpers do
  @moduledoc """
  Shared helper functions for building DescribeGroups requests across all versions.

  Version differences:
  - V0-V2: `groups` (array of group IDs) -- all schema-identical
  - V3-V5: Adds `include_authorized_operations` boolean (KIP-430)
  - V5: Flexible version (KIP-482) with compact encoding -- Kayrock handles encoding
  """

  @doc """
  Builds a DescribeGroups request by populating the request template with the groups list.
  This is shared logic between V0-V2 requests as they have identical structure.

  ## Options
  - `group_names` (required): List of consumer group IDs to describe
  """
  @spec build_request_from_template(map(), Keyword.t()) :: map()
  def build_request_from_template(request_template, opts) do
    group_ids = Keyword.fetch!(opts, :group_names)
    Map.put(request_template, :groups, group_ids)
  end

  @doc """
  Builds a DescribeGroups V3+ request by populating the request template with groups list
  plus `include_authorized_operations` (KIP-430).

  V3/V4 add `include_authorized_operations: :boolean`. V5 is the flexible version (KIP-482)
  with the same logical fields -- Kayrock handles compact encoding.

  ## Options
  - `group_names` (required): List of consumer group IDs to describe
  - `include_authorized_operations` (optional): Whether to include authorized operations (defaults to false)
  """
  @spec build_v3_plus_request(map(), Keyword.t()) :: map()
  def build_v3_plus_request(request_template, opts) do
    include_authorized_operations = Keyword.get(opts, :include_authorized_operations, false)

    request_template
    |> build_request_from_template(opts)
    |> Map.put(:include_authorized_operations, include_authorized_operations)
  end
end
