defmodule KafkaEx.Protocol.Kayrock.OffsetCommit.ResponseHelpers do
  @moduledoc """
  Shared helper functions for parsing OffsetCommit responses across all versions.

  All versions (V0-V3) share the same response parsing logic - only the semantics differ:
  - V0: Zookeeper-based offset commit
  - V1: Kafka/Zookeeper-based with consumer group coordination
  - V2: Kafka-based offset commit (recommended)
  - V3: Adds throttle_time_ms field (not currently used in response)

  Response only contains partition and error_code (no offset returned).
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
  def parse_response(%{responses: responses}) do
    responses
    |> fail_fast_iterate_topics(&parse_partition_responses/2)
    |> build_response()
  end

  # ------------------------------------------------------------------------------
  # Private Functions
  # ------------------------------------------------------------------------------

  defp parse_partition_responses(topic, partition_responses) do
    fail_fast_iterate_partitions(partition_responses, topic, &build_commit_result/2)
  end

  defp build_commit_result(topic, %{partition: partition, error_code: 0}) do
    data = %{partition: partition, error_code: :no_error}
    {:ok, Offset.from_list_offset(topic, [data])}
  end

  defp build_commit_result(topic, %{error_code: error_code, partition: partition}) do
    {:error, {error_code, topic, partition}}
  end
end
