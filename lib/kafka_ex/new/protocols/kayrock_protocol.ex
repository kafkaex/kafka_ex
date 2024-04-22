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
  Builds kayrock request based on type, api version and opts
  """
  @impl KafkaEx.New.Client.Protocol
  def build_request(:describe_groups, api_version, opts) do
    api_version
    |> Kayrock.DescribeGroups.get_request_struct()
    |> KayrockProtocol.DescribeGroups.Request.build_request(opts)
  end

  def build_request(:list_offsets, api_version, opts) do
    api_version
    |> Kayrock.ListOffsets.get_request_struct()
    |> KayrockProtocol.ListOffsets.Request.build_request(opts)
  end

  # -----------------------------------------------------------------------------
  @doc """
  Parses response based on request type and response
  """
  @impl KafkaEx.New.Client.Protocol
  def parse_response(:describe_groups, response) do
    KayrockProtocol.DescribeGroups.Response.parse_response(response)
  end

  def parse_response(:list_offsets, response) do
    KayrockProtocol.ListOffsets.Response.parse_response(response)
  end
end
