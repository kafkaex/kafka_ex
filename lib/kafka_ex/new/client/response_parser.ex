defmodule KafkaEx.New.Client.ResponseParser do
  @moduledoc """
  This module is used to parse response from KafkaEx.New.Client.
  It's main decision point which protocol to use for parsing response
  """
  alias KafkaEx.New.Client.Error
  alias KafkaEx.New.Kafka.ApiVersions
  alias KafkaEx.New.Kafka.ClusterMetadata
  alias KafkaEx.New.Kafka.ConsumerGroupDescription
  alias KafkaEx.New.Kafka.Heartbeat
  alias KafkaEx.New.Kafka.JoinGroup
  alias KafkaEx.New.Kafka.LeaveGroup
  alias KafkaEx.New.Kafka.Offset
  alias KafkaEx.New.Kafka.RecordMetadata
  alias KafkaEx.New.Kafka.SyncGroup

  @protocol Application.compile_env(:kafka_ex, :protocol, KafkaEx.New.Protocols.KayrockProtocol)

  @doc """
  Parses response for ApiVersions API
  """
  @spec api_versions_response(term) :: {:ok, ApiVersions.t()} | {:error, Error.t()}
  def api_versions_response(response) do
    @protocol.parse_response(:api_versions, response)
  end

  @doc """
  Parses response for Describe Groups API
  """
  @spec describe_groups_response(term) :: {:ok, [ConsumerGroupDescription.t()]} | {:error, term}
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
  Parses response for JoinGroup API
  """
  @spec join_group_response(term) :: {:ok, JoinGroup.t()} | {:error, Error.t()}
  def join_group_response(response) do
    @protocol.parse_response(:join_group, response)
  end

  @doc """
  Parses response for LeaveGroup API
  """
  @spec leave_group_response(term) :: {:ok, :no_error | LeaveGroup.t()} | {:error, Error.t()}
  def leave_group_response(response) do
    @protocol.parse_response(:leave_group, response)
  end

  @doc """
  Parses response for SyncGroup API
  """
  @spec sync_group_response(term) :: {:ok, SyncGroup.t()} | {:error, Error.t()}
  def sync_group_response(response) do
    @protocol.parse_response(:sync_group, response)
  end

  @doc """
  Parses response for Metadata API
  """
  @spec metadata_response(term) :: {:ok, ClusterMetadata.t()} | {:error, Error.t()}
  def metadata_response(response) do
    @protocol.parse_response(:metadata, response)
  end

  @doc """
  Parses response for Produce API
  """
  @spec produce_response(term) :: {:ok, RecordMetadata.t()} | {:error, Error.t()}
  def produce_response(response) do
    @protocol.parse_response(:produce, response)
  end
end
