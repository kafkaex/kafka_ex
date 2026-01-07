defmodule KafkaEx.Protocol.Kayrock.ApiVersions do
  @moduledoc """
  This module handles ApiVersions request & response parsing.

  ApiVersions is a special API used during client initialization to discover
  which Kafka API versions the broker supports. This enables dynamic version
  negotiation for all other Kafka APIs.
  """

  defprotocol Request do
    @moduledoc """
    This protocol is used to build ApiVersions requests.
    """

    @spec build_request(t(), Keyword.t()) :: t()
    def build_request(request, opts)
  end

  defprotocol Response do
    @moduledoc """
    This protocol is used to parse ApiVersions responses.
    """

    alias KafkaEx.Client.Error
    alias KafkaEx.Messages.ApiVersions

    @spec parse_response(t()) :: {:ok, ApiVersions.t()} | {:error, Error.t()}
    def parse_response(response)
  end
end
