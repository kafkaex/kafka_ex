defmodule KafkaEx.Protocol.Kayrock.DescribeGroups.ResponseHelpers do
  @moduledoc """
  Shared helper functions for parsing DescribeGroups responses across all versions (V0-V5).

  All versions share the same parsing logic for the `groups` array. The domain struct
  `ConsumerGroupDescription.from_describe_group_response/1` gracefully handles optional
  fields (`authorized_operations` for V3+, `group_instance_id` per member for V4+) by
  using `Map.get/2` with nil defaults.

  Version differences in response schema:
  - V0: groups only (no throttle_time_ms)
  - V1-V2: +throttle_time_ms (groups schema identical to V0)
  - V3: +authorized_operations per group (members schema identical to V0-V2)
  - V4: +group_instance_id per member (also has authorized_operations)
  - V5: FLEX version of V4 (compact types + tagged_fields, domain fields identical)

  Note: throttle_time_ms is NOT exposed in the domain layer -- it is parsed by Kayrock
  but silently ignored in the `{:ok, [ConsumerGroupDescription.t()]}` return value.
  This is consistent with the existing V0-V1 implementation.
  """

  alias KafkaEx.Messages.ConsumerGroupDescription
  alias Kayrock.ErrorCode

  @doc """
  Parses a DescribeGroups response for any version (V0-V5).

  Returns `{:ok, [ConsumerGroupDescription.t()]}` when all groups succeed,
  or `{:error, [{group_id, error_atom}]}` when any group has a non-zero error code.

  The same function works for all versions because:
  - All versions have a `groups` array with `error_code` and `group_id`
  - `ConsumerGroupDescription.from_describe_group_response/1` uses `Map.get/2`
    for optional fields (authorized_operations, group_instance_id)
  """
  @spec parse_response(map()) ::
          {:ok, [ConsumerGroupDescription.t()]} | {:error, [{String.t(), atom()}]}
  def parse_response(%{groups: groups}) do
    case Enum.filter(groups, &(&1.error_code != 0)) do
      [] ->
        groups = Enum.map(groups, &build_consumer_group/1)
        {:ok, groups}

      errors ->
        error_list =
          Enum.map(errors, fn %{group_id: group_id, error_code: error_code} ->
            {group_id, ErrorCode.code_to_atom(error_code)}
          end)

        {:error, error_list}
    end
  end

  defp build_consumer_group(kayrock_group) do
    ConsumerGroupDescription.from_describe_group_response(kayrock_group)
  end
end
