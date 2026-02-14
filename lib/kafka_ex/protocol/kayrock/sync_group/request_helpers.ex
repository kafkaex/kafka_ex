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
    raw_assignment = Keyword.get(opts, :group_assignment, [])

    %{
      group_id: Keyword.fetch!(opts, :group_id),
      generation_id: Keyword.fetch!(opts, :generation_id),
      member_id: Keyword.fetch!(opts, :member_id),
      group_assignment: convert_group_assignment(raw_assignment)
    }
  end

  @doc """
  Builds a SyncGroup request by populating the request template with extracted fields.
  This is shared logic between V0-V2 requests as they have identical structure.
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
    |> Map.put(:assignments, group_assignment)
  end

  @doc """
  Builds a SyncGroup V3+ request by populating the request template with common fields
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

  # Converts group assignment from protocol-agnostic format to Kayrock structs.
  # Accepts multiple input formats for flexibility:
  # 1. Already Kayrock format (passthrough): [%{member_id: ..., member_assignment: %Kayrock.MemberAssignment{}}]
  # 2. Simple format: [%{member_id: ..., topic_partitions: [{topic, [partitions]}]}]
  defp convert_group_assignment(assignments) when is_list(assignments) do
    Enum.map(assignments, &convert_member_assignment/1)
  end

  defp convert_member_assignment(%{member_id: _member_id, assignment: %Kayrock.MemberAssignment{}} = assignment) do
    # Already in Kayrock format, passthrough
    assignment
  end

  defp convert_member_assignment(%{member_id: member_id, topic_partitions: topic_partitions}) do
    # Convert from simple format to Kayrock format
    %{
      member_id: member_id,
      assignment: %Kayrock.MemberAssignment{
        version: 0,
        partition_assignments:
          Enum.map(topic_partitions, fn {topic, partitions} ->
            %Kayrock.MemberAssignment.PartitionAssignment{
              topic: topic,
              partitions: partitions
            }
          end),
        user_data: ""
      }
    }
  end

  defp convert_member_assignment(%{member_id: _} = assignment) do
    # Other map format, passthrough (handles edge cases)
    assignment
  end
end
