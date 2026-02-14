defmodule KafkaEx.Protocol.Kayrock.LeaveGroup.ResponseHelpers do
  @moduledoc """
  Shared helper functions for parsing LeaveGroup responses across all versions (V0-V4).

  Version differences:
  - V0: No throttle_time_ms, no members. Returns `{:ok, :no_error}` on success.
  - V1-V2: Adds throttle_time_ms. Returns `{:ok, %LeaveGroup{throttle_time_ms: ...}}`.
  - V3-V4: Adds `members` array with per-member error codes (KIP-345 batch leave).
    Returns `{:ok, %LeaveGroup{throttle_time_ms: ..., members: [...]}}`.
  - V4: Flexible version (KIP-482) -- Kayrock handles compact encoding/decoding
    transparently. Domain-relevant fields are identical to V3.
  """

  alias KafkaEx.Client.Error
  alias KafkaEx.Messages.LeaveGroup
  alias Kayrock.ErrorCode

  @doc """
  Parses a V0 response (no throttle_time_ms, no members).

  Returns `{:ok, :no_error}` on success to preserve the existing V0 contract.
  """
  @spec parse_v0_response(map()) :: {:ok, :no_error} | {:error, Error.t()}
  def parse_v0_response(response) do
    case ErrorCode.code_to_atom(response.error_code) do
      :no_error -> {:ok, :no_error}
      error_atom -> {:error, Error.build(error_atom, %{})}
    end
  end

  @doc """
  Parses a V1/V2 response (includes throttle_time_ms, no members).

  Returns `{:ok, %LeaveGroup{}}` on success with throttle_time_ms populated.
  """
  @spec parse_v1_v2_response(map()) :: {:ok, LeaveGroup.t()} | {:error, Error.t()}
  def parse_v1_v2_response(response) do
    case ErrorCode.code_to_atom(response.error_code) do
      :no_error ->
        {:ok, LeaveGroup.build(throttle_time_ms: response.throttle_time_ms)}

      error_atom ->
        {:error, Error.build(error_atom, %{})}
    end
  end

  @doc """
  Parses a V3+ response (includes throttle_time_ms and members array).

  Returns `{:ok, %LeaveGroup{throttle_time_ms: ..., members: [...]}}` on success.
  Each member in the `members` list has:
  - `member_id` - the member ID
  - `group_instance_id` - the static membership instance ID (nil for dynamic)
  - `error` - the error code as an atom (e.g., `:no_error`, `:unknown_member_id`)

  The top-level `error_code` is checked first. If it indicates an error, the
  per-member results are not included in the error response.
  """
  @spec parse_v3_plus_response(map()) :: {:ok, LeaveGroup.t()} | {:error, Error.t()}
  def parse_v3_plus_response(response) do
    case ErrorCode.code_to_atom(response.error_code) do
      :no_error ->
        members = parse_members(response.members)

        {:ok,
         LeaveGroup.build(
           throttle_time_ms: response.throttle_time_ms,
           members: members
         )}

      error_atom ->
        {:error, Error.build(error_atom, %{})}
    end
  end

  # --- Private ---

  defp parse_members(members) when is_list(members) do
    Enum.map(members, fn member ->
      %{
        member_id: member.member_id,
        group_instance_id: member.group_instance_id,
        error: ErrorCode.code_to_atom(member.error_code)
      }
    end)
  end

  defp parse_members(_), do: []
end
