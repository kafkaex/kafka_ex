defmodule KafkaEx.New.Protocols.Kayrock.Heartbeat do
  @moduledoc """
  This module handles Heartbeat request & response parsing.
  Request is built using Kayrock protocol, response is parsed to
  native KafkaEx structs.
  """

  defprotocol Request do
    @moduledoc """
    This protocol is used to build Heartbeat requests
    """
    @spec build_request(t(), Keyword.t()) :: t()
    def build_request(request, opts)
  end

  defprotocol Response do
    @moduledoc """
    This protocol is used to parse Heartbeat responses.

    ## Return Values
    - V0: `{:ok, :no_error}` on success
    - V1: `{:ok, Heartbeat.t()}` on success (includes throttle_time_ms)
    - All versions: `{:error, Error.t()}` on error
    """
    alias KafkaEx.New.Structs.Error
    alias KafkaEx.New.Structs.Heartbeat

    @spec parse_response(t()) :: {:ok, :no_error | Heartbeat.t()} | {:error, Error.t()}
    def parse_response(response)
  end
end
