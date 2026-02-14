defmodule KafkaEx.Protocol.Kayrock.SyncGroup do
  @moduledoc """
  This module handles SyncGroup request & response parsing.
  Request is built using Kayrock protocol, response is parsed to
  native KafkaEx structs.

  ## Supported Versions

  - **V0**: Basic SyncGroup
    - Request: `group_id`, `generation_id`, `member_id`, `assignments`
    - Response: `error_code`, `assignment`
  - **V1**: Adds throttle_time_ms to response
    - Request: Same as V0
    - Response: +`throttle_time_ms`
  - **V2**: No changes vs V1 (pure version bump)
  - **V3**: Static membership (KIP-345)
    - Request: +`group_instance_id`
    - Response: Same as V1/V2
  - **V4**: Flexible version (KIP-482)
    - Request: Compact string encoding, +`tagged_fields`
    - Response: Compact bytes encoding, +`tagged_fields`
    - Domain-relevant fields identical to V3

  All known versions (V0-V4) have explicit `defimpl` implementations.
  An `Any` fallback is retained for forward compatibility with unknown
  future Kayrock versions.
  """

  defprotocol Request do
    @moduledoc """
    This protocol is used to build SyncGroup requests.

    ## V0-V2 Options
    - `group_id` (required): The consumer group ID
    - `generation_id` (required): The generation ID from JoinGroup
    - `member_id` (required): The member ID
    - `group_assignment` (optional): List of member assignments (leader only)

    ## V3-V4 Options (adds to V0-V2)
    - `group_instance_id` (optional): Static membership instance ID (nil for dynamic)

    All known versions (V0-V4) have explicit implementations.
    The `Any` fallback handles unknown future versions.
    """
    @fallback_to_any true

    @spec build_request(t(), Keyword.t()) :: t()
    def build_request(request, opts)
  end

  defprotocol Response do
    @moduledoc """
    This protocol is used to parse SyncGroup responses.

    ## Return Values
    - All versions: `{:ok, SyncGroup.t()}` on success
    - All versions: `{:error, Error.t()}` on error

    All known versions (V0-V4) have explicit implementations.
    The `Any` fallback handles unknown future versions.
    """
    @fallback_to_any true

    alias KafkaEx.Client.Error
    alias KafkaEx.Messages.SyncGroup

    @spec parse_response(t()) :: {:ok, SyncGroup.t()} | {:error, Error.t()}
    def parse_response(response)
  end
end
