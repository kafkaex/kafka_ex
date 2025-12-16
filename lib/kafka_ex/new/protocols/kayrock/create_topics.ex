defmodule KafkaEx.New.Protocols.Kayrock.CreateTopics do
  @moduledoc """
  This module handles CreateTopics request & response parsing.
  Request is built using Kayrock protocol, response is parsed to
  native KafkaEx structs.

  ## Supported Versions

  - V0: Basic topic creation
  - V1: Adds validate_only flag and error_message in response
  - V2: Adds throttle_time_ms in response
  """

  defprotocol Request do
    @moduledoc """
    This protocol is used to build CreateTopics requests.
    """
    @spec build_request(t(), Keyword.t()) :: t()
    def build_request(request, opts)
  end

  defprotocol Response do
    @moduledoc """
    This protocol is used to parse CreateTopics responses.

    ## Return Values
    - All versions: `{:ok, CreateTopics.t()}` on success (may contain per-topic errors)
    - All versions: `{:error, Error.t()}` on request-level error
    """
    alias KafkaEx.New.Client.Error
    alias KafkaEx.New.Kafka.CreateTopics

    @spec parse_response(t()) :: {:ok, CreateTopics.t()} | {:error, Error.t()}
    def parse_response(response)
  end
end
