defmodule KafkaEx.Protocol.Kayrock.DeleteTopics do
  @moduledoc """
  Protocol definitions for DeleteTopics API.

  Kayrock supports V0-V4:
  - V0: Basic topic deletion
  - V1: Adds throttle_time_ms in response
  - V2: No changes vs V1 (identical request + response schemas)
  - V3: No changes vs V2 (identical request + response schemas)
  - V4: FLEX version (KIP-482) — compact types + tagged_fields. Kayrock handles
    encoding/decoding transparently. Domain fields identical to V1-V3.
  """

  alias KafkaEx.Client.Error
  alias KafkaEx.Messages.DeleteTopics

  defprotocol Request do
    @moduledoc """
    Protocol for building DeleteTopics requests.
    """

    @fallback_to_any true

    @doc """
    Builds a DeleteTopics request from the given options.

    ## Options

    - `:topics` - List of topic names to delete (required)
    - `:timeout` - Request timeout in milliseconds (required)
    """
    @spec build_request(t(), Keyword.t()) :: t()
    def build_request(request, opts)
  end

  defprotocol Response do
    @moduledoc """
    Protocol for parsing DeleteTopics responses.
    """

    @fallback_to_any true

    @doc """
    Parses a DeleteTopics response into a DeleteTopics struct.
    """
    @spec parse_response(t()) :: {:ok, DeleteTopics.t()} | {:error, Error.t()}
    def parse_response(response)
  end
end
