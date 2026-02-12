defmodule KafkaEx.Protocol.KayrockProtocol do
  @moduledoc """
  This module handles Kayrock request & response handling & parsing.
  Once Kafka Ex v1.0 is released, this module will be renamed to KayrockProtocol
  and will become a separated package.
  """

  alias KafkaEx.Protocol.Kayrock, as: KayrockProtocol

  alias Kayrock.ApiVersions
  alias Kayrock.CreateTopics
  alias Kayrock.DeleteTopics
  alias Kayrock.DescribeGroups
  alias Kayrock.ErrorCode
  alias Kayrock.Fetch
  alias Kayrock.FindCoordinator
  alias Kayrock.Heartbeat
  alias Kayrock.JoinGroup
  alias Kayrock.KafkaSchemaMetadata
  alias Kayrock.LeaveGroup
  alias Kayrock.ListOffsets
  alias Kayrock.Metadata
  alias Kayrock.OffsetCommit
  alias Kayrock.OffsetFetch
  alias Kayrock.Produce
  alias Kayrock.Request
  alias Kayrock.SyncGroup

  # -----------------------------------------------------------------------------
  @doc """
  Extracts operation name and API version from a Kayrock request struct.

  Returns `{operation, api_version}` tuple where:
    - `operation` is an atom like `:metadata`, `:produce`, `:fetch`, etc.
    - `api_version` is an integer like 0, 1, 2, etc.
  """
  @spec request_info(struct()) :: {atom(), non_neg_integer()}
  def request_info(request) do
    api_key = request.__struct__.api_key()
    api_version = request.__struct__.api_vsn()
    operation = KafkaSchemaMetadata.api_key(api_key)
    {operation, api_version}
  rescue
    _ -> {:unknown, 0}
  end

  # -----------------------------------------------------------------------------
  @doc """
  Builds kayrock request based on type, api version and opts
  """
  def build_request(:api_versions, api_version, opts) do
    api_version
    |> ApiVersions.get_request_struct()
    |> KayrockProtocol.ApiVersions.Request.build_request(opts)
  end

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

  def build_request(:join_group, api_version, opts) do
    api_version
    |> JoinGroup.get_request_struct()
    |> KayrockProtocol.JoinGroup.Request.build_request(opts)
  end

  def build_request(:leave_group, api_version, opts) do
    api_version
    |> LeaveGroup.get_request_struct()
    |> KayrockProtocol.LeaveGroup.Request.build_request(opts)
  end

  def build_request(:sync_group, api_version, opts) do
    api_version
    |> SyncGroup.get_request_struct()
    |> KayrockProtocol.SyncGroup.Request.build_request(opts)
  end

  def build_request(:metadata, api_version, opts) do
    api_version
    |> Metadata.get_request_struct()
    |> KayrockProtocol.Metadata.Request.build_request(opts)
  end

  def build_request(:produce, api_version, opts) do
    api_version
    |> Produce.get_request_struct()
    |> KayrockProtocol.Produce.Request.build_request(opts)
  end

  def build_request(:fetch, api_version, opts) do
    api_version
    |> Fetch.get_request_struct()
    |> KayrockProtocol.Fetch.Request.build_request(opts)
  end

  def build_request(:find_coordinator, api_version, opts) do
    api_version
    |> FindCoordinator.get_request_struct()
    |> KayrockProtocol.FindCoordinator.Request.build_request(opts)
  end

  def build_request(:create_topics, api_version, opts) do
    api_version
    |> CreateTopics.get_request_struct()
    |> KayrockProtocol.CreateTopics.Request.build_request(opts)
  end

  def build_request(:delete_topics, api_version, opts) do
    api_version
    |> DeleteTopics.get_request_struct()
    |> KayrockProtocol.DeleteTopics.Request.build_request(opts)
  end

  # -----------------------------------------------------------------------------
  @doc """
  Parses response based on request type and response
  """
  def parse_response(:api_versions, response), do: KayrockProtocol.ApiVersions.Response.parse_response(response)
  def parse_response(:describe_groups, response), do: KayrockProtocol.DescribeGroups.Response.parse_response(response)
  def parse_response(:list_offsets, response), do: KayrockProtocol.ListOffsets.Response.parse_response(response)
  def parse_response(:offset_fetch, response), do: KayrockProtocol.OffsetFetch.Response.parse_response(response)
  def parse_response(:offset_commit, response), do: KayrockProtocol.OffsetCommit.Response.parse_response(response)
  def parse_response(:heartbeat, response), do: KayrockProtocol.Heartbeat.Response.parse_response(response)
  def parse_response(:join_group, response), do: KayrockProtocol.JoinGroup.Response.parse_response(response)
  def parse_response(:leave_group, response), do: KayrockProtocol.LeaveGroup.Response.parse_response(response)
  def parse_response(:sync_group, response), do: KayrockProtocol.SyncGroup.Response.parse_response(response)
  def parse_response(:metadata, response), do: KayrockProtocol.Metadata.Response.parse_response(response)
  def parse_response(:produce, response), do: KayrockProtocol.Produce.Response.parse_response(response)
  def parse_response(:fetch, response), do: KayrockProtocol.Fetch.Response.parse_response(response)
  def parse_response(:find_coordinator, response), do: KayrockProtocol.FindCoordinator.Response.parse_response(response)
  def parse_response(:create_topics, response), do: KayrockProtocol.CreateTopics.Response.parse_response(response)
  def parse_response(:delete_topics, response), do: KayrockProtocol.DeleteTopics.Response.parse_response(response)

  # -----------------------------------------------------------------------------
  # Utility wrappers — allow code outside lib/kafka_ex/protocol/ to avoid
  # importing Kayrock modules directly.
  # -----------------------------------------------------------------------------

  @doc """
  Returns the integer API key for a given API name atom (e.g. `:metadata` → 3).
  """
  @spec api_key(atom()) :: non_neg_integer()
  def api_key(api_name) when is_atom(api_name), do: KafkaSchemaMetadata.api_key(api_name)

  @doc """
  Returns the maximum protocol version that Kayrock supports for the given API.
  """
  @spec max_supported_version(atom()) :: non_neg_integer()
  def max_supported_version(api_name) when is_atom(api_name) do
    {_, max} = KafkaSchemaMetadata.version_range(api_name)
    max
  end

  @doc """
  Converts an integer Kafka error code to its atom representation.
  """
  @spec error_code_to_atom(integer()) :: atom()
  def error_code_to_atom(code) when is_integer(code), do: ErrorCode.code_to_atom(code)

  @doc """
  Converts an error atom to its integer Kafka error code.
  """
  @spec atom_to_error_code!(atom()) :: integer()
  def atom_to_error_code!(atom) when is_atom(atom), do: ErrorCode.atom_to_code!(atom)

  @doc """
  Serializes a Kayrock request struct to wire-format iodata.
  """
  @spec serialize_request(struct()) :: iodata()
  def serialize_request(request), do: Request.serialize(request)

  @doc """
  Returns the response deserializer function for a given request struct.
  """
  @spec response_deserializer(struct()) :: (binary() -> {struct(), binary()})
  def response_deserializer(request), do: Request.response_deserializer(request)
end
