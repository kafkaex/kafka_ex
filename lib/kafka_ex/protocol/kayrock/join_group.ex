defmodule KafkaEx.Protocol.Kayrock.JoinGroup do
  @moduledoc """
  This module handles JoinGroup request & response parsing.
  Request is built using Kayrock protocol, response is parsed to
  native KafkaEx structs.

  ## Supported Versions

  - **V0**: Basic JoinGroup with session_timeout
    - Request: `group_id`, `session_timeout_ms`, `member_id`, `protocol_type`, `protocols`
    - Response: `error_code`, `generation_id`, `protocol_name`, `leader`, `member_id`, `members`
  - **V1**: Adds rebalance_timeout
    - Request: +`rebalance_timeout_ms`
    - Response: Same as V0
  - **V2**: Adds throttle_time_ms to response
    - Request: Same as V1
    - Response: +`throttle_time_ms`
  - **V3**: No changes vs V2 (pure version bump)
  - **V4**: No changes vs V3 (pure version bump)
  - **V5**: Static membership (KIP-345)
    - Request: +`group_instance_id`
    - Response: +`group_instance_id` per member (not extracted to domain layer)
  - **V6**: Flexible version (KIP-482)
    - Request: Compact string encoding, +`tagged_fields`
    - Response: Compact string encoding, +`tagged_fields`
    - Domain-relevant fields identical to V5

  All known versions (V0-V6) have explicit `defimpl` implementations.
  An `Any` fallback is retained for forward compatibility with unknown
  future Kayrock versions.
  """

  defprotocol Request do
    @moduledoc """
    This protocol is used to build JoinGroup requests.

    ## V0 Options
    - `group_id` (required): The consumer group ID
    - `session_timeout` (required): Session timeout in milliseconds
    - `member_id` (required): The member ID (empty string for new members)
    - `group_protocols` or `topics` (required): Group protocols or topics list
    - `protocol_type` (optional): Protocol type, defaults to "consumer"

    ## V1-V4 Options (adds to V0)
    - `rebalance_timeout` (required): Rebalance timeout in milliseconds

    ## V5-V6 Options (adds to V1-V4)
    - `group_instance_id` (optional): Static membership instance ID (nil for dynamic)

    All known versions (V0-V6) have explicit implementations.
    The `Any` fallback handles unknown future versions.
    """
    @fallback_to_any true

    @spec build_request(t(), Keyword.t()) :: t()
    def build_request(request, opts)
  end

  defprotocol Response do
    @moduledoc """
    This protocol is used to parse JoinGroup responses.

    ## Return Values
    - All versions: `{:ok, JoinGroup.t()}` on success
    - All versions: `{:error, Error.t()}` on error

    All known versions (V0-V6) have explicit implementations.
    The `Any` fallback handles unknown future versions.
    """
    @fallback_to_any true

    alias KafkaEx.Client.Error
    alias KafkaEx.Messages.JoinGroup

    @spec parse_response(t()) :: {:ok, JoinGroup.t()} | {:error, Error.t()}
    def parse_response(response)
  end
end
