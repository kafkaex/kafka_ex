defmodule KafkaEx.Protocol.Kayrock.OffsetCommit.ResponseHelpers do
  @moduledoc """
  Shared helper functions for parsing OffsetCommit responses across all versions (V0-V8).

  All versions share the same response parsing logic — only the wire format semantics differ.
  Response only contains partition and error_code (no offset returned).
  V8 is a flexible version (KIP-482) with tagged_fields, handled transparently by Kayrock.
  """

  import KafkaEx.Protocol.Kayrock.ResponseHelpers,
    only: [build_response: 1, fail_fast_iterate_topics: 2, fail_fast_iterate_partitions: 3]

  alias KafkaEx.Messages.Offset

  @type error_tuple :: {atom(), String.t(), non_neg_integer()}

  @doc """
  Parses an OffsetCommit response.

  All versions share the same response parsing logic.
  """
  @spec parse_response(map()) :: {:ok, list(Offset.t())} | {:error, any()}
  def parse_response(%{topics: topics}) do
    topics
    |> fail_fast_iterate_topics(&parse_partition_responses/2)
    |> build_response()
  end

  # ------------------------------------------------------------------------------
  # Private Functions
  # ------------------------------------------------------------------------------

  defp parse_partition_responses(topic, partition_responses) do
    fail_fast_iterate_partitions(partition_responses, topic, &build_commit_result/2)
  end

  defp build_commit_result(topic, %{partition_index: partition, error_code: 0}) do
    data = %{partition: partition, error_code: :no_error}
    {:ok, Offset.from_list_offset(topic, [data])}
  end

  defp build_commit_result(topic, %{error_code: error_code, partition_index: partition}) do
    {:error, {error_code, topic, partition}}
  end
end
