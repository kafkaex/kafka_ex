defmodule KafkaEx.New.Protocols.KayrockProtocol do
  @moduledoc """
  This module handles Kayrock request & response handling & parsing.
  Once Kafka Ex v1.0 is released, this module will be renamed to KayrockProtocol
  and will become a separated package.
  """
  @behaviour KafkaEx.New.Client.Protocol

  alias KafkaEx.New.Protocols.Kayrock, as: KayrockProtocol

  # -----------------------------------------------------------------------------
  @doc """
  Builds request for Describe Groups API
  """
  @impl KafkaEx.New.Client.Protocol
  def build_request(:describe_groups, api_version, opts) do
    group_names = Keyword.fetch!(opts, :group_names)

    api_version
    |> Kayrock.DescribeGroups.get_request_struct()
    |> KayrockProtocol.DescribeGroups.Request.build_request(group_names)
  end

  # -----------------------------------------------------------------------------
  @doc """
  Parses response for Describe Groups API
  """
  @impl KafkaEx.New.Client.Protocol
  def parse_response(:describe_groups, response) do
    KayrockProtocol.DescribeGroups.Response.parse_response(response)
  end
end
