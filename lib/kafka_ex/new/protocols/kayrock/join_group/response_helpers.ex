defmodule KafkaEx.New.Protocols.Kayrock.JoinGroup.ResponseHelpers do
  @moduledoc """
  Shared helper functions for parsing JoinGroup responses across all versions.

  Version differences:
  - V0/V1: No throttle_time_ms field
  - V2: Adds throttle_time_ms to the response
  """

  alias KafkaEx.New.Client.Error
  alias KafkaEx.New.Kafka.JoinGroup
  alias Kayrock.ErrorCode

  @type throttle_time_extractor :: (map() -> non_neg_integer() | nil)

  @doc """
  Parses a JoinGroup response using the provided throttle_time extractor.
  """
  @spec parse_response(map(), throttle_time_extractor()) ::
          {:ok, JoinGroup.t()} | {:error, Error.t()}
  def parse_response(response, throttle_time_extractor) do
    %{
      error_code: error_code,
      generation_id: generation_id,
      group_protocol: group_protocol,
      leader_id: leader_id,
      member_id: member_id,
      members: members
    } = response

    case ErrorCode.code_to_atom(error_code) do
      :no_error ->
        parsed_members = extract_members(members)

        {:ok,
         JoinGroup.build(
           throttle_time_ms: throttle_time_extractor.(response),
           generation_id: generation_id,
           group_protocol: group_protocol,
           leader_id: leader_id,
           member_id: member_id,
           members: parsed_members
         )}

      error_atom ->
        {:error, Error.build(error_atom, %{})}
    end
  end

  # ------------------------------------------------------------------------------
  # Private Functions
  # ------------------------------------------------------------------------------

  defp extract_members(kayrock_members) when is_list(kayrock_members) do
    Enum.map(kayrock_members, fn member ->
      %JoinGroup.Member{
        member_id: member.member_id,
        member_metadata: member.member_metadata
      }
    end)
  end

  defp extract_members(_), do: []
end
