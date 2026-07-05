defmodule KafkaEx.Protocol.Kayrock.ListGroups do
  @moduledoc """
  This module handles List Groups request & response parsing.
  Request is built using the Kayrock protocol; the response is parsed to
  native `KafkaEx.Messages.ConsumerGroupListing` structs.

  ListGroups asks a single broker for the consumer groups it coordinates. A
  cluster-wide listing (fan-out across all brokers + merge) is assembled in
  `KafkaEx.API.list_groups/2`.

  ## Supported Versions

  - **V0**: Basic ListGroups
    - Request: no fields
    - Response: `error_code` (top-level) + `groups` (array of `group_id`, `protocol_type`)
  - **V1**: Adds `throttle_time_ms` to response (not exposed in the domain layer)
    - Request: same as V0
  - **V2**: No changes vs V1 (pure version bump)
  - **V3**: Flexible version (KIP-482)
    - Request: `tagged_fields` only (Kayrock handles compact encoding)
    - Response: compact encoding + `tagged_fields`; domain fields identical to V0

  All known versions (V0-V3) have explicit `defimpl` implementations.
  An `Any` fallback is retained for forward compatibility with unknown
  future Kayrock versions.
  """

  defprotocol Request do
    @moduledoc """
    This protocol is used to build List Groups requests.

    ListGroups carries no request fields at any version, so every
    implementation returns the request template unchanged. The `opts`
    argument is accepted for signature parity with other operations.

    All known versions (V0-V3) have explicit implementations.
    The `Any` fallback handles unknown future versions.
    """
    @fallback_to_any true

    @spec build_request(t(), Keyword.t()) :: t()
    def build_request(request, opts)
  end

  defprotocol Response do
    @moduledoc """
    This protocol is used to parse List Groups responses.

    ## Return Values
    - Success: `{:ok, [ConsumerGroupListing.t()]}`
    - Error: `{:error, atom}` (the top-level error code as an atom)

    All known versions (V0-V3) have explicit implementations.
    The `Any` fallback handles unknown future versions.
    """
    @fallback_to_any true

    alias KafkaEx.Messages.ConsumerGroupListing

    @spec parse_response(t()) :: {:ok, [ConsumerGroupListing.t()]} | {:error, atom}
    def parse_response(response)
  end
end
