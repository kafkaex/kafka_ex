defmodule KafkaEx.Protocol.Kayrock.OffsetFetch do
  @moduledoc """
  This module handles Offset Fetch request & response parsing.
  Request is built using Kayrock protocol, response is parsed to
  native KafkaEx structs.

  ## Supported Versions

  - V0: Basic fetch from Zookeeper
  - V1: Coordinator-based fetch from Kafka (recommended)
  - V2: Enhanced error handling (adds top-level error_code)
  - V3: Includes throttle time
  - V4: No changes vs V3 (pure version bump)
  - V5: Response adds `committed_leader_epoch` per partition
  - V6: Flexible version (KIP-482) -- compact encodings + tagged fields

  All known versions (V0-V6) have explicit `defimpl` implementations.
  An `Any` fallback is retained for forward compatibility with unknown
  future Kayrock versions.
  """

  defprotocol Request do
    @moduledoc """
    This protocol is used to build Offset Fetch requests.

    Implementations must populate the request struct with:
    - `group_id` - The consumer group ID
    - `topics` - List of topic/partition data

    All known versions (V0-V6) have explicit implementations.
    The `Any` fallback handles unknown future versions.
    """
    @fallback_to_any true

    @spec build_request(t(), Keyword.t()) :: t()
    def build_request(request, opts)
  end

  defprotocol Response do
    @moduledoc """
    This protocol is used to parse Offset Fetch responses.

    ## Return Values

    - `{:ok, [Offset.t()]}` on success with offset data
    - `{:error, term}` on error with error details

    All known versions (V0-V6) have explicit implementations.
    The `Any` fallback handles unknown future versions.
    """
    @fallback_to_any true

    alias KafkaEx.Messages.Offset

    @spec parse_response(t()) :: {:ok, [Offset.t()]} | {:error, term}
    def parse_response(response)
  end
end
