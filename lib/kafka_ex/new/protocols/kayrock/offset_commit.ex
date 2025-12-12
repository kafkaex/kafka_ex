defmodule KafkaEx.New.Protocols.Kayrock.OffsetCommit do
  @moduledoc """
  This module handles Offset Commit request & response parsing.
  Request is built using Kayrock protocol, response is parsed to
  native KafkaEx structs.

  The OffsetCommit API commits consumer group offsets to Kafka.
  It supports multiple versions:
  - **v0**: Basic commit to Zookeeper
  - **v1**: Adds timestamp, generation_id, member_id (Kafka/ZK storage)
  - **v2**: Removes timestamp, adds retention_time (Kafka storage, recommended)
  - **v3**: Includes throttle time
  """

  defprotocol Request do
    @moduledoc """
    This protocol is used to build Offset Commit requests
    """
    @spec build_request(t(), Keyword.t()) :: t()
    def build_request(request, opts)
  end

  defprotocol Response do
    @moduledoc """
    This protocol is used to parse Offset Commit responses
    """
    alias KafkaEx.New.Kafka.Offset

    @spec parse_response(t()) :: {:ok, [Offset.t()]} | {:error, term}
    def parse_response(response)
  end
end
