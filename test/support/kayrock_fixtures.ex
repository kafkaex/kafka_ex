defmodule KafkaEx.Test.KayrockFixtures do
  @moduledoc """
  Test fixture helpers for constructing Kayrock protocol structs.

  This module centralizes all Kayrock struct construction so that test files
  outside `test/kafka_ex/protocol/` never need to import Kayrock directly.
  This follows the architectural principle that Kayrock is an implementation
  detail of the protocol layer.

  ## Usage

      alias KafkaEx.Test.KayrockFixtures, as: Fixtures

      response = Fixtures.build_response(:offset_fetch, 1, topics: [...])
      request = Fixtures.build_request(:metadata, 1)
  """

  @doc """
  Builds a Kayrock response struct for the given API and version.

  ## Examples

      build_response(:offset_fetch, 1, topics: [...])
      build_response(:heartbeat, 0, error_code: 0)
  """
  @spec build_response(atom(), non_neg_integer(), keyword() | map()) :: struct()
  def build_response(api, version, fields \\ []) do
    module = response_module(api, version)
    fields = if is_list(fields), do: Map.new(fields), else: fields
    struct(module, fields)
  end

  @doc """
  Builds a Kayrock request struct for the given API and version.

  ## Examples

      build_request(:metadata, 1)
      build_request(:produce, 3, topic: "test")
  """
  @spec build_request(atom(), non_neg_integer(), keyword() | map()) :: struct()
  def build_request(api, version, fields \\ []) do
    module = request_module(api, version)
    fields = if is_list(fields), do: Map.new(fields), else: fields
    struct(module, fields)
  end

  @doc """
  Builds a Kayrock MemberAssignment struct.
  """
  @spec member_assignment(keyword() | map()) :: struct()
  def member_assignment(fields \\ []) do
    fields = if is_list(fields), do: Map.new(fields), else: fields
    struct(Kayrock.MemberAssignment, fields)
  end

  @doc """
  Builds a Kayrock MemberAssignment.PartitionAssignment struct.
  """
  @spec partition_assignment(keyword() | map()) :: struct()
  def partition_assignment(fields \\ []) do
    fields = if is_list(fields), do: Map.new(fields), else: fields
    struct(Kayrock.MemberAssignment.PartitionAssignment, fields)
  end

  @doc """
  Builds a Kayrock GroupProtocolMetadata struct.
  """
  @spec group_protocol_metadata(keyword() | map()) :: struct()
  def group_protocol_metadata(fields \\ []) do
    fields = if is_list(fields), do: Map.new(fields), else: fields
    struct(Kayrock.GroupProtocolMetadata, fields)
  end

  @doc """
  Builds a Kayrock MessageSet struct.
  """
  @spec message_set(keyword() | map()) :: struct()
  def message_set(fields \\ []) do
    fields = if is_list(fields), do: Map.new(fields), else: fields
    struct(Kayrock.MessageSet, fields)
  end

  @doc """
  Builds a Kayrock MessageSet.Message struct.
  """
  @spec message(keyword() | map()) :: struct()
  def message(fields \\ []) do
    fields = if is_list(fields), do: Map.new(fields), else: fields
    struct(Kayrock.MessageSet.Message, fields)
  end

  @doc """
  Builds a Kayrock RecordBatch struct.
  """
  @spec record_batch(keyword() | map()) :: struct()
  def record_batch(fields \\ []) do
    fields = if is_list(fields), do: Map.new(fields), else: fields
    struct(Kayrock.RecordBatch, fields)
  end

  @doc """
  Builds a Kayrock RecordBatch.Record struct.
  """
  @spec record(keyword() | map()) :: struct()
  def record(fields \\ []) do
    fields = if is_list(fields), do: Map.new(fields), else: fields
    struct(Kayrock.RecordBatch.Record, fields)
  end

  @doc """
  Builds a Kayrock RecordBatch.RecordHeader struct.
  """
  @spec record_header(keyword() | map()) :: struct()
  def record_header(fields \\ []) do
    fields = if is_list(fields), do: Map.new(fields), else: fields
    struct(Kayrock.RecordBatch.RecordHeader, fields)
  end

  @doc """
  Returns true if the given struct is a request of the specified API and version.

  ## Examples

      assert Fixtures.request_type?(request, :metadata, 1)
  """
  @spec request_type?(struct(), atom(), non_neg_integer()) :: boolean()
  def request_type?(%{__struct__: struct_mod}, api, version) do
    struct_mod == request_module(api, version)
  end

  @doc """
  Returns true if the given struct is a response of the specified API and version.

  ## Examples

      assert Fixtures.response_type?(response, :fetch, 3)
  """
  @spec response_type?(struct(), atom(), non_neg_integer()) :: boolean()
  def response_type?(%{__struct__: struct_mod}, api, version) do
    struct_mod == response_module(api, version)
  end

  @doc """
  Returns true if the given value is a Kayrock MessageSet struct.
  """
  @spec message_set?(term()) :: boolean()
  def message_set?(%{__struct__: Kayrock.MessageSet}), do: true
  def message_set?(_), do: false

  @doc """
  Returns true if the given value is a Kayrock RecordBatch struct.
  """
  @spec record_batch?(term()) :: boolean()
  def record_batch?(%{__struct__: Kayrock.RecordBatch}), do: true
  def record_batch?(_), do: false

  # --- Private helpers ---

  @api_modules %{
    api_versions: Kayrock.ApiVersions,
    create_topics: Kayrock.CreateTopics,
    delete_topics: Kayrock.DeleteTopics,
    describe_groups: Kayrock.DescribeGroups,
    fetch: Kayrock.Fetch,
    find_coordinator: Kayrock.FindCoordinator,
    heartbeat: Kayrock.Heartbeat,
    join_group: Kayrock.JoinGroup,
    leave_group: Kayrock.LeaveGroup,
    list_offsets: Kayrock.ListOffsets,
    metadata: Kayrock.Metadata,
    offset_commit: Kayrock.OffsetCommit,
    offset_fetch: Kayrock.OffsetFetch,
    produce: Kayrock.Produce,
    sync_group: Kayrock.SyncGroup
  }

  defp response_module(api, version) do
    base = Map.fetch!(@api_modules, api)
    Module.concat([base, :"V#{version}", Response])
  end

  defp request_module(api, version) do
    base = Map.fetch!(@api_modules, api)
    Module.concat([base, :"V#{version}", Request])
  end
end
