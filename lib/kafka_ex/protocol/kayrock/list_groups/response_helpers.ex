defmodule KafkaEx.Protocol.Kayrock.ListGroups.ResponseHelpers do
  @moduledoc """
  Shared helper for parsing ListGroups responses across all versions (V0-V3).

  Unlike DescribeGroups (per-group error codes), ListGroups reports a single
  top-level `error_code`. A non-zero code means the queried broker could not
  serve the listing.

  `throttle_time_ms` (V1+) and per-group `tagged_fields` (V3) are parsed by
  Kayrock but are not exposed in the domain layer.
  """

  alias KafkaEx.Messages.ConsumerGroupListing
  alias Kayrock.ErrorCode

  @doc """
  Parses a ListGroups response for any version (V0-V3).

  Returns `{:ok, [ConsumerGroupListing.t()]}` when the top-level `error_code`
  is 0, otherwise `{:error, error_atom}`.
  """
  @spec parse_response(map()) :: {:ok, [ConsumerGroupListing.t()]} | {:error, atom}
  def parse_response(%{error_code: 0, groups: groups}) do
    {:ok, Enum.map(groups, &ConsumerGroupListing.from_list_groups_response/1)}
  end

  def parse_response(%{error_code: error_code}) do
    {:error, ErrorCode.code_to_atom(error_code)}
  end
end
