defmodule KafkaEx.New.Protocols.Kayrock.Produce do
  @moduledoc """
  This module handles Produce request & response parsing.
  Request is built using Kayrock protocol, response is parsed to
  native KafkaEx structs.

  ## Supported Versions

  - V0: Basic produce (MessageSet format)
  - V1: Same as V0 with acks semantics clarified
  - V2: Adds timestamp support in MessageSet
  - V3: Adds transactional_id, uses RecordBatch format with headers

  ## Message Format Differences

  - **V0-V2**: Uses `Kayrock.MessageSet` with individual messages
  - **V3+**: Uses `Kayrock.RecordBatch` with records and headers support
  """

  defprotocol Request do
    @moduledoc """
    This protocol is used to build Produce requests.

    Implementations must populate the request struct with:
    - `acks` - Required acknowledgments (-1, 0, or 1)
    - `timeout` - Request timeout in milliseconds
    - `topic_data` - List of topic/partition/messages data
    - `transactional_id` (V3+ only) - Transaction ID for transactional producers
    """
    @spec build_request(t(), Keyword.t()) :: t()
    def build_request(request, opts)
  end

  defprotocol Response do
    @moduledoc """
    This protocol is used to parse Produce responses.

    ## Return Values

    - `{:ok, RecordMetadata.t()}` on success with offset and metadata
    - `{:error, Error.t()}` on error with error details
    """
    alias KafkaEx.New.Client.Error
    alias KafkaEx.New.Kafka.RecordMetadata

    @spec parse_response(t()) :: {:ok, RecordMetadata.t()} | {:error, Error.t()}
    def parse_response(response)
  end
end
