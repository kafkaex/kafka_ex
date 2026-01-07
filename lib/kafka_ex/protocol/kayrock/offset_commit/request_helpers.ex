defmodule KafkaEx.Protocol.Kayrock.OffsetCommit.RequestHelpers do
  @moduledoc """
  Shared helper functions for building OffsetCommit requests across all versions.

  This module reduces code duplication while maintaining type safety through
  protocol dispatch for each version-specific struct.
  """

  @type partition_data :: %{
          required(:partition_num) => non_neg_integer(),
          required(:offset) => integer(),
          optional(:metadata) => String.t(),
          optional(:timestamp) => integer()
        }

  @type partition_output :: %{
          required(:partition) => non_neg_integer(),
          required(:offset) => integer(),
          required(:metadata) => String.t(),
          optional(:timestamp) => integer()
        }

  @type topic_data :: {String.t(), [partition_data()]}
  @type topic_output :: %{topic: String.t(), partitions: [partition_output()]}

  @doc """
  Builds the topics structure for OffsetCommit requests.

  ## Parameters
  - `opts` - Keyword list containing `:topics` key with topic/partition data
  - `include_timestamp` - Whether to include timestamp field (v0: false, v1: true, v2+: false)

  ## Returns
  List of topic maps with partition data
  """
  @spec build_topics(Keyword.t(), boolean()) :: [topic_output()]
  def build_topics(opts, include_timestamp \\ false) do
    opts
    |> Keyword.fetch!(:topics)
    |> Enum.map(fn {topic, partitions} ->
      %{
        topic: topic,
        partitions: build_partitions(partitions, include_timestamp)
      }
    end)
  end

  @doc """
  Builds partition data for a topic.

  ## Parameters
  - `partitions` - List of partition data maps
  - `include_timestamp` - Whether to include timestamp field

  ## Returns
  List of partition maps with offset, metadata, and optionally timestamp
  """
  @spec build_partitions([partition_data()], boolean()) :: [partition_output()]
  def build_partitions(partitions, include_timestamp) do
    Enum.map(partitions, fn partition_data ->
      base = %{
        partition: partition_data.partition_num,
        offset: partition_data.offset,
        metadata: partition_data[:metadata] || ""
      }

      if include_timestamp do
        Map.put(base, :timestamp, partition_data[:timestamp] || -1)
      else
        base
      end
    end)
  end

  @doc """
  Extracts common fields from request options.
  """
  @spec extract_common_fields(Keyword.t()) :: %{group_id: String.t()}
  def extract_common_fields(opts) do
    %{group_id: Keyword.fetch!(opts, :group_id)}
  end

  @doc """
  Extracts consumer group coordination fields (generation_id, member_id).
  Used in v1+ versions.
  """
  @spec extract_coordination_fields(Keyword.t()) :: %{generation_id: integer(), member_id: String.t()}
  def extract_coordination_fields(opts) do
    %{generation_id: Keyword.get(opts, :generation_id, -1), member_id: Keyword.get(opts, :member_id, "")}
  end

  @doc """
  Extracts retention_time field for v2+ versions.
  """
  @spec extract_retention_time(Keyword.t()) :: %{retention_time: integer()}
  def extract_retention_time(opts) do
    %{retention_time: Keyword.get(opts, :retention_time, -1)}
  end

  @doc """
  Builds an OffsetCommit v2/v3 request with all fields.
  """
  @spec build_v2_v3_request(struct(), Keyword.t()) :: struct()
  def build_v2_v3_request(request_template, opts) do
    %{group_id: group_id} = extract_common_fields(opts)
    %{generation_id: generation_id, member_id: member_id} = extract_coordination_fields(opts)
    %{retention_time: retention_time} = extract_retention_time(opts)
    topics = build_topics(opts, false)

    request_template
    |> Map.put(:group_id, group_id)
    |> Map.put(:generation_id, generation_id)
    |> Map.put(:member_id, member_id)
    |> Map.put(:retention_time, retention_time)
    |> Map.put(:topics, topics)
  end
end
