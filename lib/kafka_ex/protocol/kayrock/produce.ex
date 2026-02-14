defmodule KafkaEx.Protocol.Kayrock.Produce do
  @moduledoc """
  This module handles Produce request & response parsing.
  Request is built using Kayrock protocol, response is parsed to
  native KafkaEx structs.

  ## Supported Versions

  - V0: Basic produce (MessageSet format)
  - V1: Same as V0, clarified acks semantics
  - V2: Adds timestamp support in MessageSet, adds `log_append_time` in response
  - V3: Adds `transactional_id`, uses RecordBatch format with headers
  - V4: Same as V3, transactional/idempotent producer support
  - V5: Response adds `log_start_offset`
  - V6: No changes vs V5
  - V7: No changes vs V6
  - V8: Response adds `record_errors` and `error_message` per partition (KIP-467)

  ## Message Format Differences

  - **V0-V2**: Uses `Kayrock.MessageSet` with individual messages
  - **V3+**: Uses `Kayrock.RecordBatch` with records and headers support

  All known versions (V0-V8) have explicit `defimpl` implementations.
  An `Any` fallback is retained for forward compatibility with unknown
  future Kayrock versions.
  """

  defprotocol Request do
    @moduledoc """
    This protocol is used to build Produce requests.

    Implementations must populate the request struct with:
    - `acks` - Required acknowledgments (-1, 0, or 1)
    - `timeout` - Request timeout in milliseconds
    - `topic_data` - List of topic/partition/messages data
    - `transactional_id` (V3+ only) - Transaction ID for transactional producers

    All known versions (V0-V8) have explicit implementations.
    The `Any` fallback handles unknown future versions.
    """
    @fallback_to_any true

    @spec build_request(t(), Keyword.t()) :: t()
    def build_request(request, opts)
  end

  defprotocol Response do
    @moduledoc """
    This protocol is used to parse Produce responses.

    ## Return Values

    - `{:ok, RecordMetadata.t()}` on success with offset and metadata
    - `{:error, Error.t()}` on error with error details

    All known versions (V0-V8) have explicit implementations.
    The `Any` fallback handles unknown future versions.
    """
    @fallback_to_any true

    alias KafkaEx.Client.Error
    alias KafkaEx.Messages.RecordMetadata

    @spec parse_response(t()) :: {:ok, RecordMetadata.t()} | {:error, Error.t()}
    def parse_response(response)
  end
end
