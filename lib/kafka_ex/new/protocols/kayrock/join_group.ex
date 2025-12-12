defmodule KafkaEx.New.Protocols.Kayrock.JoinGroup do
  @moduledoc """
  This module handles JoinGroup request & response parsing.
  Request is built using Kayrock protocol, response is parsed to
  native KafkaEx structs.
  """

  defprotocol Request do
    @moduledoc """
    This protocol is used to build JoinGroup requests
    """
    @spec build_request(t(), Keyword.t()) :: t()
    def build_request(request, opts)
  end

  defprotocol Response do
    @moduledoc """
    This protocol is used to parse JoinGroup responses.

    ## Return Values
    - All versions: `{:ok, JoinGroup.t()}` on success
    - All versions: `{:error, Error.t()}` on error
    """
    alias KafkaEx.New.Client.Error
    alias KafkaEx.New.Kafka.JoinGroup

    @spec parse_response(t()) :: {:ok, JoinGroup.t()} | {:error, Error.t()}
    def parse_response(response)
  end
end
