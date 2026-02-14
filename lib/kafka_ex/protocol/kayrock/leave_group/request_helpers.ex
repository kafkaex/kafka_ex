defmodule KafkaEx.Protocol.Kayrock.LeaveGroup.RequestHelpers do
  @moduledoc """
  Shared helper functions for building LeaveGroup requests across all versions.

  Version differences:
  - V0-V2: `group_id`, `member_id` (all schema-identical)
  - V3-V4: **STRUCTURAL CHANGE** (KIP-345 batch leave) -- replaces `member_id`
    with `members` array. Each member has `member_id` and `group_instance_id`.
  - V4: Flexible version (KIP-482) with compact encoding -- Kayrock handles encoding
  """

  @doc """
  Extracts common fields from request options (V0-V2).
  """
  @spec extract_common_fields(Keyword.t()) :: %{group_id: String.t(), member_id: String.t()}
  def extract_common_fields(opts) do
    %{
      group_id: Keyword.fetch!(opts, :group_id),
      member_id: Keyword.fetch!(opts, :member_id)
    }
  end

  @doc """
  Builds a LeaveGroup V0-V2 request by populating the request template with extracted fields.
  This is shared logic between V0-V2 requests as they have identical structure.
  """
  @spec build_request_from_template(map(), Keyword.t()) :: map()
  def build_request_from_template(request_template, opts) do
    %{group_id: group_id, member_id: member_id} = extract_common_fields(opts)

    request_template
    |> Map.put(:group_id, group_id)
    |> Map.put(:member_id, member_id)
  end

  @doc """
  Builds a LeaveGroup V3+ request (KIP-345 batch leave).

  V3 introduces a **structural change**: the single `member_id` field is replaced
  with a `members` array. Each member in the array has:
  - `member_id` (required)
  - `group_instance_id` (optional, for static membership)

  V4 is the flexible version (KIP-482) with the same logical fields -- Kayrock
  handles compact encoding.

  ## Options
  - `group_id` (required): The consumer group ID
  - `members` (required): List of member maps, each with `:member_id` and
    optionally `:group_instance_id`
  """
  @spec build_v3_plus_request(map(), Keyword.t()) :: map()
  def build_v3_plus_request(request_template, opts) do
    group_id = Keyword.fetch!(opts, :group_id)
    members = Keyword.fetch!(opts, :members)

    members_with_defaults =
      Enum.map(members, fn member ->
        %{
          member_id: Map.fetch!(member, :member_id),
          group_instance_id: Map.get(member, :group_instance_id, nil)
        }
      end)

    request_template
    |> Map.put(:group_id, group_id)
    |> Map.put(:members, members_with_defaults)
  end
end
