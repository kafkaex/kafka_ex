defmodule KafkaEx.Protocol.Kayrock.OffsetFetch.ResponseHelpers do
  @moduledoc """
  Shared helper functions for parsing OffsetFetch responses across all versions.

  Version differences:
  - V0/V1: No top-level error_code field
  - V2/V3: Includes top-level error_code for broker-level errors
  """

  import KafkaEx.Protocol.Kayrock.ResponseHelpers,
    only: [build_response: 1, fail_fast_iterate_topics: 2, fail_fast_iterate_partitions: 3]

  alias KafkaEx.Client.Error, as: ErrorStruct
  alias KafkaEx.Messages.Offset

  @type error_tuple :: {atom(), String.t(), non_neg_integer()}

  @doc """
  Parses an OffsetFetch response without top-level error_code (V0/V1).
  """
  @spec parse_response_without_top_level_error(map()) :: {:ok, list(Offset.t())} | {:error, any()}
  def parse_response_without_top_level_error(%{topics: topics}) do
    topics
    |> fail_fast_iterate_topics(&parse_partition_responses/2)
    |> build_response()
  end

  @doc """
  Parses an OffsetFetch response with top-level error_code (V2/V3).
  """
  @spec parse_response_with_top_level_error(map()) :: {:ok, list(Offset.t())} | {:error, any()}
  def parse_response_with_top_level_error(%{error_code: error_code}) when error_code != 0 do
    {:error, ErrorStruct.build(error_code, %{})}
  end

  def parse_response_with_top_level_error(%{error_code: 0, topics: topics}) do
    topics
    |> fail_fast_iterate_topics(&parse_partition_responses/2)
    |> build_response()
  end

  # ------------------------------------------------------------------------------
  # Private Functions
  # ------------------------------------------------------------------------------

  defp parse_partition_responses(topic, partition_responses) do
    fail_fast_iterate_partitions(partition_responses, topic, &build_offset/2)
  end

  defp build_offset(topic, %{partition_index: partition, error_code: 0, committed_offset: offset, metadata: metadata}) do
    data = %{
      partition: partition,
      offset: offset,
      error_code: :no_error,
      metadata: metadata || ""
    }

    {:ok, Offset.from_list_offset(topic, [data])}
  end

  defp build_offset(topic, %{error_code: error_code, partition_index: partition}) do
    {:error, {error_code, topic, partition}}
  end
end
