defmodule KafkaEx.New.Protocols.Kayrock.OffsetFetch do
  @moduledoc """
  This module handles Offset Fetch request & response parsing.
  Request is built using Kayrock protocol, response is parsed to
  native KafkaEx structs.

  The OffsetFetch API retrieves committed consumer group offsets from Kafka.
  It supports multiple versions:
  - **v0**: Basic fetch from Zookeeper
  - **v1**: Coordinator-based fetch from Kafka (recommended)
  - **v2**: Enhanced error handling
  - **v3**: Includes throttle time
  """

  defprotocol Request do
    @moduledoc """
    This protocol is used to build Offset Fetch requests
    """
    @spec build_request(t(), Keyword.t()) :: t()
    def build_request(request, opts)
  end

  defprotocol Response do
    @moduledoc """
    This protocol is used to parse Offset Fetch responses
    """
    alias KafkaEx.New.Structs.Offset

    @spec parse_response(t()) :: {:ok, [Offset.t()]} | {:error, term}
    def parse_response(response)
  end
end
