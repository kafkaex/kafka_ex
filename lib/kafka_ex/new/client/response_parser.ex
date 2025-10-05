defmodule KafkaEx.New.Client.ResponseParser do
  @moduledoc """
  This module is used to parse response from KafkaEx.New.Client.
  It's main decision point which protocol to use for parsing response
  """
  alias KafkaEx.New.Structs.ConsumerGroup
  alias KafkaEx.New.Structs.Error
  alias KafkaEx.New.Structs.Heartbeat
  alias KafkaEx.New.Structs.LeaveGroup
  alias KafkaEx.New.Structs.Offset

  @protocol Application.compile_env(:kafka_ex, :protocol, KafkaEx.New.Protocols.KayrockProtocol)

  @doc """
  Parses response for Describe Groups API
  """
  @spec describe_groups_response(term) :: {:ok, [ConsumerGroup.t()]} | {:error, term}
  def describe_groups_response(response) do
    @protocol.parse_response(:describe_groups, response)
  end

  @doc """
  Parses response for List Groups API
  """
  @spec list_offsets_response(term) :: {:ok, [Offset.t()]} | {:error, Error.t()}
  def list_offsets_response(response) do
    @protocol.parse_response(:list_offsets, response)
  end

  @doc """
  Parses response for Offset Fetch API
  """
  @spec offset_fetch_response(term) :: {:ok, [Offset.t()]} | {:error, Error.t()}
  def offset_fetch_response(response) do
    @protocol.parse_response(:offset_fetch, response)
  end

  @doc """
  Parses response for Offset Commit API
  """
  @spec offset_commit_response(term) :: {:ok, [Offset.t()]} | {:error, Error.t()}
  def offset_commit_response(response) do
    @protocol.parse_response(:offset_commit, response)
  end

  @doc """
  Parses response for Heartbeat API
  """
  @spec heartbeat_response(term) :: {:ok, :no_error | Heartbeat.t()} | {:error, Error.t()}
  def heartbeat_response(response) do
    @protocol.parse_response(:heartbeat, response)
  end

  @doc """
  Parses response for LeaveGroup API
  """
  @spec leave_group_response(term) :: {:ok, :no_error | LeaveGroup.t()} | {:error, Error.t()}
  def leave_group_response(response) do
    @protocol.parse_response(:leave_group, response)
  end
end
