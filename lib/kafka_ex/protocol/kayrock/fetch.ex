defmodule KafkaEx.Protocol.Kayrock.Fetch do
  @moduledoc """
  This module handles Fetch request & response parsing.
  Request is built using Kayrock protocol, response is parsed to
  native KafkaEx structs.

  ## Supported Versions

  - V0: Basic fetch (MessageSet format)
  - V1: Adds throttle_time_ms
  - V2: Same as V1
  - V3: Adds max_bytes at request level
  - V4: Adds isolation_level, last_stable_offset, aborted_transactions
  - V5: Adds log_start_offset in request and response
  - V6: Same as V5
  - V7: Adds session_id, epoch, forgotten_topics_data for incremental fetch
  - V8: No changes vs V7
  - V9: Adds current_leader_epoch in partition requests
  - V10: No changes vs V9
  - V11: Adds rack_id at request level; response adds preferred_read_replica

  ## Message Format

  - **V0-V3**: May return `Kayrock.MessageSet` (legacy format)
  - **V4+**: Returns `Kayrock.RecordBatch` (modern format with headers)

  The response parser handles both formats transparently.

  All known versions (V0-V11) have explicit `defimpl` implementations.
  An `Any` fallback is retained for forward compatibility with unknown
  future Kayrock versions.
  """

  defprotocol Request do
    @moduledoc """
    This protocol is used to build Fetch requests.

    Implementations must populate the request struct with:
    - `replica_id` - Should be -1 for consumers
    - `max_wait_time` - Maximum time to wait for messages in ms
    - `min_bytes` - Minimum bytes to accumulate before returning
    - `topics` - List of topic/partition/offset data to fetch
    - `max_bytes` (V3+) - Maximum bytes to return
    - `isolation_level` (V4+) - 0 for READ_UNCOMMITTED, 1 for READ_COMMITTED
    - `log_start_offset` (V5+) - Log start offset for partitions
    - `session_id`, `epoch` (V7+) - For incremental fetch sessions
    - `current_leader_epoch` (V9+) - Epoch of the current leader for partitions
    - `rack_id` (V11+) - Consumer rack ID for rack-aware fetch

    All known versions (V0-V11) have explicit implementations.
    The `Any` fallback handles unknown future versions.
    """
    @fallback_to_any true

    @spec build_request(t(), Keyword.t()) :: t()
    def build_request(request, opts)
  end

  defprotocol Response do
    @moduledoc """
    This protocol is used to parse Fetch responses.

    ## Return Values

    - `{:ok, Fetch.t()}` on success with messages and metadata
    - `{:error, Error.t()}` on error with error details

    All known versions (V0-V11) have explicit implementations.
    The `Any` fallback handles unknown future versions.
    """
    @fallback_to_any true

    alias KafkaEx.Client.Error
    alias KafkaEx.Messages.Fetch

    @spec parse_response(t()) :: {:ok, Fetch.t()} | {:error, Error.t()}
    def parse_response(response)
  end
end
