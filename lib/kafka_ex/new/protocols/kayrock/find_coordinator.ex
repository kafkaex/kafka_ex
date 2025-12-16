defmodule KafkaEx.New.Protocols.Kayrock.FindCoordinator do
  @moduledoc """
  This module handles FindCoordinator request & response parsing.
  Request is built using Kayrock protocol, response is parsed to
  native KafkaEx structs.

  ## Supported Versions

  - **V0**: Basic group coordinator discovery
    - Request: `group_id`
    - Response: `error_code`, `coordinator` (node_id, host, port)
  - **V1**: Extended coordinator discovery with type support
    - Request: `coordinator_key`, `coordinator_type` (0=group, 1=transaction)
    - Response: Adds `throttle_time_ms`, `error_message`

  ## Usage

  FindCoordinator is primarily used to discover which broker is the coordinator
  for a consumer group (type=0) or transaction (type=1).
  """

  defprotocol Request do
    @moduledoc """
    This protocol is used to build FindCoordinator requests.

    ## V0 Options
    - `group_id` (required): The consumer group ID

    ## V1 Options
    - `coordinator_key` (required): The key to look up (group_id or transactional_id)
    - `coordinator_type` (optional): 0 for group (default), 1 for transaction
    """
    @spec build_request(t(), Keyword.t()) :: t()
    def build_request(request, opts)
  end

  defprotocol Response do
    @moduledoc """
    This protocol is used to parse FindCoordinator responses.

    ## Return Values

    - `{:ok, FindCoordinator.t()}` on success with coordinator info
    - `{:error, Error.t()}` on error with error details
    """
    alias KafkaEx.New.Client.Error
    alias KafkaEx.New.Kafka.FindCoordinator

    @spec parse_response(t()) :: {:ok, FindCoordinator.t()} | {:error, Error.t()}
    def parse_response(response)
  end
end
