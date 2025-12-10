defmodule KafkaEx.New.Protocols.Kayrock.SyncGroup do
  @moduledoc """
  This module handles SyncGroup request & response parsing.
  Request is built using Kayrock protocol, response is parsed to
  native KafkaEx structs.
  """

  defprotocol Request do
    @moduledoc """
    This protocol is used to build SyncGroup requests
    """
    @spec build_request(t(), Keyword.t()) :: t()
    def build_request(request, opts)
  end

  defprotocol Response do
    @moduledoc """
    This protocol is used to parse SyncGroup responses.
    """
    alias KafkaEx.New.Structs.Error
    alias KafkaEx.New.Structs.SyncGroup

    @spec parse_response(t()) :: {:ok, SyncGroup.t()} | {:error, Error.t()}
    def parse_response(response)
  end
end
