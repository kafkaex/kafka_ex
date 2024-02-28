defmodule KafkaEx.New.Client.RequestBuilder do
  @moduledoc """
  This module is used to build request for KafkaEx.New.Client.
  It's main decision point which protocol to use for building request and what
  is required version.
  """
  @protocol Application.compile_env(
              :kafka_ex,
              :protocol,
              KafkaEx.New.Protocols.KayrockProtocol
            )

  @default_api_version %{
    describe_groups: 1
  }

  alias KafkaEx.New.Client.State

  @doc """
  Builds request for Describe Groups API
  """
  @spec describe_groups_request([binary], State.t()) :: term
  def describe_groups_request(group_names, state) do
    api_version = get_api_version(state, :describe_groups)

    @protocol.build_request(:describe_groups, api_version, group_names: group_names)
  end

  # -----------------------------------------------------------------------------
  defp get_api_version(state, request_type) do
    default = Map.fetch!(@default_api_version, request_type)
    State.max_supported_api_version(state, request_type, default)
  end
end
