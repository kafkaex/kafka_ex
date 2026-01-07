defmodule KafkaEx.Protocol.Kayrock.LeaveGroup do
  @moduledoc """
  This module handles LeaveGroup request & response parsing.
  Request is built using Kayrock protocol, response is parsed to
  native KafkaEx structs.
  """

  defprotocol Request do
    @moduledoc """
    This protocol is used to build LeaveGroup requests
    """
    @spec build_request(t(), Keyword.t()) :: t()
    def build_request(request, opts)
  end

  defprotocol Response do
    @moduledoc """
    This protocol is used to parse LeaveGroup responses.

    ## Return Values
    - V0: `{:ok, :no_error}` on success
    - V1: `{:ok, LeaveGroup.t()}` on success (includes throttle_time_ms)
    - All versions: `{:error, Error.t()}` on error
    """
    alias KafkaEx.Client.Error
    alias KafkaEx.Messages.LeaveGroup

    @spec parse_response(t()) :: {:ok, :no_error | LeaveGroup.t()} | {:error, Error.t()}
    def parse_response(response)
  end
end
