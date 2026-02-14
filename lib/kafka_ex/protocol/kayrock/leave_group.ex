defmodule KafkaEx.Protocol.Kayrock.LeaveGroup do
  @moduledoc """
  This module handles LeaveGroup request & response parsing.
  Request is built using Kayrock protocol, response is parsed to
  native KafkaEx structs.

  ## Supported Versions

  - **V0**: Basic LeaveGroup
    - Request: `group_id`, `member_id`
    - Response: `error_code`
  - **V1**: Adds throttle_time_ms to response
    - Request: Same as V0
    - Response: +`throttle_time_ms`
  - **V2**: No changes vs V1 (pure version bump)
  - **V3**: Batch leave (KIP-345) -- **STRUCTURAL CHANGE**
    - Request: -`member_id`, +`members` array (each with `member_id`, `group_instance_id`)
    - Response: +`members` array with per-member `error_code`
  - **V4**: Flexible version (KIP-482)
    - Request: Compact string encoding, +`tagged_fields`
    - Response: Compact encoding, +`tagged_fields`
    - Domain-relevant fields identical to V3

  All known versions (V0-V4) have explicit `defimpl` implementations.
  An `Any` fallback is retained for forward compatibility with unknown
  future Kayrock versions.
  """

  defprotocol Request do
    @moduledoc """
    This protocol is used to build LeaveGroup requests.

    ## V0-V2 Options
    - `group_id` (required): The consumer group ID
    - `member_id` (required): The member ID

    ## V3-V4 Options (STRUCTURAL CHANGE -- batch leave)
    - `group_id` (required): The consumer group ID
    - `members` (required): List of member maps, each with `:member_id` and
      optionally `:group_instance_id`

    All known versions (V0-V4) have explicit implementations.
    The `Any` fallback handles unknown future versions.
    """
    @fallback_to_any true

    @spec build_request(t(), Keyword.t()) :: t()
    def build_request(request, opts)
  end

  defprotocol Response do
    @moduledoc """
    This protocol is used to parse LeaveGroup responses.

    ## Return Values
    - V0: `{:ok, :no_error}` on success
    - V1-V2: `{:ok, LeaveGroup.t()}` on success (includes throttle_time_ms)
    - V3+: `{:ok, LeaveGroup.t()}` on success (includes throttle_time_ms and members)
    - All versions: `{:error, Error.t()}` on error

    All known versions (V0-V4) have explicit implementations.
    The `Any` fallback handles unknown future versions.
    """
    @fallback_to_any true

    alias KafkaEx.Client.Error
    alias KafkaEx.Messages.LeaveGroup

    @spec parse_response(t()) :: {:ok, :no_error | LeaveGroup.t()} | {:error, Error.t()}
    def parse_response(response)
  end
end
