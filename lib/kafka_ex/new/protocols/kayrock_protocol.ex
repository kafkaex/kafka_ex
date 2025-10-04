defmodule KafkaEx.New.Protocols.KayrockProtocol do
  @moduledoc """
  This module handles Kayrock request & response handling & parsing.
  Once Kafka Ex v1.0 is released, this module will be renamed to KayrockProtocol
  and will become a separated package.
  """
  @behaviour KafkaEx.New.Client.Protocol

  alias KafkaEx.New.Protocols.Kayrock, as: KayrockProtocol
  alias Kayrock.DescribeGroups
  alias Kayrock.Heartbeat
  alias Kayrock.ListOffsets
  alias Kayrock.OffsetCommit
  alias Kayrock.OffsetFetch

  # -----------------------------------------------------------------------------
  @doc """
  Builds kayrock request based on type, api version and opts
  """
  @impl KafkaEx.New.Client.Protocol
  def build_request(:describe_groups, api_version, opts) do
    api_version
    |> DescribeGroups.get_request_struct()
    |> KayrockProtocol.DescribeGroups.Request.build_request(opts)
  end

  def build_request(:list_offsets, api_version, opts) do
    api_version
    |> ListOffsets.get_request_struct()
    |> KayrockProtocol.ListOffsets.Request.build_request(opts)
  end

  def build_request(:offset_fetch, api_version, opts) do
    api_version
    |> OffsetFetch.get_request_struct()
    |> KayrockProtocol.OffsetFetch.Request.build_request(opts)
  end

  def build_request(:offset_commit, api_version, opts) do
    api_version
    |> OffsetCommit.get_request_struct()
    |> KayrockProtocol.OffsetCommit.Request.build_request(opts)
  end

  def build_request(:heartbeat, api_version, opts) do
    api_version
    |> Heartbeat.get_request_struct()
    |> KayrockProtocol.Heartbeat.Request.build_request(opts)
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

  def parse_response(:offset_fetch, response) do
    KayrockProtocol.OffsetFetch.Response.parse_response(response)
  end

  def parse_response(:offset_commit, response) do
    KayrockProtocol.OffsetCommit.Response.parse_response(response)
  end

  def parse_response(:heartbeat, response) do
    KayrockProtocol.Heartbeat.Response.parse_response(response)
  end
end
