defmodule KafkaEx.Protocol.Kayrock.DeleteTopics do
  @moduledoc """
  Protocol definitions for DeleteTopics API.

  Kayrock supports V0 and V1:
  - V0: Basic topic deletion
  - V1: Adds throttle_time_ms in response
  """

  alias KafkaEx.Messages.DeleteTopics
  alias KafkaEx.Client.Error

  defprotocol Request do
    @moduledoc """
    Protocol for building DeleteTopics requests.
    """

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

    @doc """
    Parses a DeleteTopics response into a DeleteTopics struct.
    """
    @spec parse_response(t()) :: {:ok, DeleteTopics.t()} | {:error, Error.t()}
    def parse_response(response)
  end
end
