defmodule KafkaEx.Protocol.Kayrock.ListOffsets do
  @moduledoc """
  This module handles List Offsets request & response parsing.
  Request is built using Kayrock protocol, response is parsed to
  native KafkaEx structs.

  ## Supported Versions

  - V0: Uses `offsets` array in response, `max_num_offsets` in request
  - V1: Single `offset` field in response (replaces offsets array)
  - V2: Adds `isolation_level` in request, `throttle_time_ms` + `timestamp` in response
  - V3: Adds `current_leader_epoch` in request partitions (Kayrock schema same as V2)
  - V4: Request adds `current_leader_epoch` in partitions; response adds `leader_epoch`
  - V5: No changes vs V4

  All known versions (V0-V5) have explicit `defimpl` implementations.
  An `Any` fallback is retained for forward compatibility with unknown
  future Kayrock versions.
  """

  defprotocol Request do
    @moduledoc """
    This protocol is used to build Lists Offsets request.

    Implementations must populate the request struct with:
    - `replica_id` - Should be -1 for consumers
    - `topics` - List of topic/partition/timestamp data
    - `isolation_level` (V2+) - 0 for READ_UNCOMMITTED, 1 for READ_COMMITTED
    - `current_leader_epoch` (V4+ in Kayrock schema) - Epoch for fencing (-1 = unknown)

    All known versions (V0-V5) have explicit implementations.
    The `Any` fallback handles unknown future versions.
    """
    @fallback_to_any true

    @spec build_request(t(), Keyword.t()) :: t()
    def build_request(request, opts)
  end

  defprotocol Response do
    @moduledoc """
    This protocol is used to parse Lists Offsets response.

    ## Return Values

    - `{:ok, [Offset.t()]}` on success with offset data
    - `{:error, term}` on error with error details

    All known versions (V0-V5) have explicit implementations.
    The `Any` fallback handles unknown future versions.
    """
    @fallback_to_any true

    alias KafkaEx.Messages.Offset

    @spec parse_response(t()) :: {:ok, [Offset.t()]} | {:error, term}
    def parse_response(response)
  end
end
