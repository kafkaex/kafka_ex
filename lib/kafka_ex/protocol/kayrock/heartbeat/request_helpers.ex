defmodule KafkaEx.Protocol.Kayrock.Heartbeat.RequestHelpers do
  @moduledoc """
  Shared helper functions for building Heartbeat requests across all versions.

  Version differences:
  - V0-V2: `group_id`, `generation_id`, `member_id` (all schema-identical)
  - V3-V4: Adds `group_instance_id` for static membership (KIP-345)
  - V4: Flexible version (KIP-482) with compact encoding -- Kayrock handles encoding
  """

  @doc """
  Extracts common fields from request options.
  """
  @spec extract_common_fields(Keyword.t()) :: %{
          group_id: String.t(),
          member_id: String.t(),
          generation_id: non_neg_integer()
        }
  def extract_common_fields(opts) do
    %{
      group_id: Keyword.fetch!(opts, :group_id),
      member_id: Keyword.fetch!(opts, :member_id),
      generation_id: Keyword.fetch!(opts, :generation_id)
    }
  end

  @doc """
  Builds a Heartbeat request by populating the request template with extracted fields.
  This is shared logic between V0-V2 requests as they have identical structure.
  """
  @spec build_request_from_template(map(), Keyword.t()) :: map()
  def build_request_from_template(request_template, opts) do
    %{group_id: group_id, member_id: member_id, generation_id: generation_id} =
      extract_common_fields(opts)

    request_template
    |> Map.put(:group_id, group_id)
    |> Map.put(:member_id, member_id)
    |> Map.put(:generation_id, generation_id)
  end

  @doc """
  Builds a Heartbeat V3+ request by populating the request template with common fields
  plus `group_instance_id` for static membership (KIP-345).

  V3 adds `group_instance_id: :nullable_string`. V4 is the flexible version (KIP-482)
  with the same logical fields -- Kayrock handles compact encoding.
  """
  @spec build_v3_plus_request(map(), Keyword.t()) :: map()
  def build_v3_plus_request(request_template, opts) do
    group_instance_id = Keyword.get(opts, :group_instance_id, nil)

    request_template
    |> build_request_from_template(opts)
    |> Map.put(:group_instance_id, group_instance_id)
  end
end
