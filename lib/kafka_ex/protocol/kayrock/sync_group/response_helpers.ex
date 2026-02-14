defmodule KafkaEx.Protocol.Kayrock.SyncGroup.ResponseHelpers do
  @moduledoc """
  Shared helper functions for parsing SyncGroup responses across all versions (V0-V4).

  Version differences:
  - V0: No throttle_time_ms field
  - V1-V3: Adds throttle_time_ms to the response
  - V4: Flexible version (KIP-482) -- Kayrock handles compact bytes deserialization
        transparently. The `assignment` field may be raw binary instead of
        `%Kayrock.MemberAssignment{}` -- `extract_partition_assignments/1` handles both.
  """

  alias Kayrock.ErrorCode
  alias KafkaEx.Client.Error
  alias KafkaEx.Messages.SyncGroup
  alias KafkaEx.Messages.ConsumerGroupDescription.Member.MemberAssignment.PartitionAssignment

  @doc """
  Parses a V0 response (no throttle_time_ms).
  """
  @spec parse_v0_response(map()) :: {:ok, SyncGroup.t()} | {:error, Error.t()}
  def parse_v0_response(response) do
    parse_response(response, fn _resp -> nil end)
  end

  @doc """
  Parses a V1+ response (includes throttle_time_ms).
  """
  @spec parse_v1_plus_response(map()) :: {:ok, SyncGroup.t()} | {:error, Error.t()}
  def parse_v1_plus_response(response) do
    parse_response(response, fn resp -> resp.throttle_time_ms end)
  end

  @doc """
  Parses a SyncGroup response using the provided throttle_time extractor.
  """
  @spec parse_response(map(), (map() -> non_neg_integer() | nil)) ::
          {:ok, SyncGroup.t()} | {:error, Error.t()}
  def parse_response(response, throttle_time_extractor) do
    %{error_code: error_code, assignment: member_assignment} = response

    case ErrorCode.code_to_atom(error_code) do
      :no_error ->
        partition_assignments = extract_partition_assignments(member_assignment)

        {:ok,
         SyncGroup.build(
           throttle_time_ms: throttle_time_extractor.(response),
           partition_assignments: partition_assignments
         )}

      error_atom ->
        {:error, Error.build(error_atom, %{})}
    end
  end

  @doc """
  Extracts partition assignments from a member assignment.

  Handles both `%Kayrock.MemberAssignment{}` structs (V0-V3) and raw binary
  or other formats (V4 compact_bytes or edge cases). Returns empty list for
  unrecognized formats.
  """
  @spec extract_partition_assignments(any()) :: [PartitionAssignment.t()]
  def extract_partition_assignments(%Kayrock.MemberAssignment{partition_assignments: kayrock_assignments}) do
    Enum.map(kayrock_assignments, fn %Kayrock.MemberAssignment.PartitionAssignment{
                                       topic: topic,
                                       partitions: partitions
                                     } ->
      %PartitionAssignment{
        topic: topic,
        partitions: partitions
      }
    end)
  end

  def extract_partition_assignments(_), do: []
end
