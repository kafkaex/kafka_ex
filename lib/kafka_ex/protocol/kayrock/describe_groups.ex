defmodule KafkaEx.Protocol.Kayrock.DescribeGroups do
  @moduledoc """
  This module handles Describe Groups request & response parsing.
  Request is built using Kayrock protocol, response is parsed to
  native KafkaEx structs.

  ## Supported Versions

  - **V0**: Basic DescribeGroups
    - Request: `groups` (array of group IDs)
    - Response: `groups` (array with error_code, group_id, group_state, protocol_type, protocol_data, members)
  - **V1**: Adds throttle_time_ms to response
    - Request: Same as V0
    - Response: +`throttle_time_ms`
  - **V2**: No changes vs V1 (pure version bump)
  - **V3**: Adds include_authorized_operations to request (KIP-430)
    - Request: +`include_authorized_operations`
    - Response: +`authorized_operations` per group
  - **V4**: Adds group_instance_id per member (KIP-345)
    - Request: Same as V3
    - Response: +`group_instance_id` per member
  - **V5**: Flexible version (KIP-482)
    - Request: Compact string encoding, +`tagged_fields`
    - Response: Compact encoding, +`tagged_fields`
    - Domain-relevant fields identical to V4

  All known versions (V0-V5) have explicit `defimpl` implementations.
  An `Any` fallback is retained for forward compatibility with unknown
  future Kayrock versions.
  """

  defprotocol Request do
    @moduledoc """
    This protocol is used to build Describe Groups requests.

    ## V0-V2 Options
    - `group_names` (required): List of consumer group IDs to describe

    ## V3-V5 Options (adds to V0-V2)
    - `include_authorized_operations` (optional): Whether to include authorized operations (defaults to false)

    All known versions (V0-V5) have explicit implementations.
    The `Any` fallback handles unknown future versions.
    """
    @fallback_to_any true

    @spec build_request(t(), Keyword.t()) :: t()
    def build_request(request, opts)
  end

  defprotocol Response do
    @moduledoc """
    This protocol is used to parse Describe Groups responses.

    ## Return Values
    - Success: `{:ok, [ConsumerGroupDescription.t()]}`
    - Error: `{:error, [{String.t(), atom()}]}`

    All known versions (V0-V5) have explicit implementations.
    The `Any` fallback handles unknown future versions.
    """
    @fallback_to_any true

    alias KafkaEx.Messages.ConsumerGroupDescription

    @spec parse_response(t()) :: {:ok, [ConsumerGroupDescription.t()]} | {:error, term}
    def parse_response(response)
  end
end
