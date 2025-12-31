defmodule KafkaEx.Protocol.Kayrock.OffsetFetch.RequestHelpers do
  @moduledoc """
  Shared helper functions for building OffsetFetch requests across all versions.

  This module reduces code duplication while maintaining type safety through
  protocol dispatch for each version-specific struct.
  """

  @type partition_data :: %{partition_num: non_neg_integer()}
  @type topic_data :: {String.t(), [partition_data()]}
  @type partition_output :: %{partition: non_neg_integer()}
  @type topic_output :: %{topic: String.t(), partitions: [partition_output()]}

  @doc """
  Builds the topics structure for OffsetFetch requests.
  """
  @spec build_topics(Keyword.t()) :: [topic_output()]
  def build_topics(opts) do
    opts
    |> Keyword.fetch!(:topics)
    |> Enum.map(fn {topic, partitions} ->
      %{
        topic: topic,
        partitions: build_partitions(partitions)
      }
    end)
  end

  @doc """
  Builds partition data for a topic.
  """
  @spec build_partitions([partition_data()]) :: [partition_output()]
  def build_partitions(partitions) do
    Enum.map(partitions, fn partition_data ->
      %{partition: partition_data.partition_num}
    end)
  end

  @doc """
  Extracts common fields from request options.
  """
  @spec extract_common_fields(Keyword.t()) :: %{group_id: String.t()}
  def extract_common_fields(opts) do
    %{
      group_id: Keyword.fetch!(opts, :group_id)
    }
  end
end
