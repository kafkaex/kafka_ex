defmodule KafkaEx.Protocol.Kayrock.OffsetCommit.ResponseHelpers do
  @moduledoc """
  Shared helper functions for parsing OffsetCommit responses across all versions (V0-V10).

  All versions share the same response parsing logic — only the wire format semantics differ.
  Response only contains partition and error_code (no offset returned).
  V8 is a flexible version (KIP-482) with tagged_fields, handled transparently by Kayrock.
  V10 identifies topics by `topic_id`; the name must be attached first with
  `put_topic_names/2`.
  """

  import KafkaEx.Protocol.Kayrock.ResponseHelpers,
    only: [build_response: 1, check_topic_names: 1, fail_fast_iterate_topics: 2, fail_fast_iterate_partitions: 3]

  alias KafkaEx.Protocol.Kayrock.ResponseHelpers, as: SharedHelpers

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

  @doc """
  Parses an OffsetCommit V10+ response, whose topics carry `topic_id` instead
  of `name`.

  Each topic must have had its `name` attached first (`put_topic_names/2`);
  a topic whose id could not be resolved yields `{:error, :unknown_topic_id}`.
  """
  @spec parse_topic_id_response(map()) :: {:ok, list(Offset.t())} | {:error, any()}
  def parse_topic_id_response(%{topics: topics} = response) do
    with :ok <- check_topic_names(topics) do
      parse_response(response)
    end
  end

  @doc """
  Attaches topic names to the id-only topics of an OffsetCommit V10+ response.
  A no-op for earlier versions, whose topics already carry names.
  """
  @spec put_topic_names(map(), KafkaEx.Protocol.Kayrock.ResponseHelpers.topic_name_resolver()) :: map()
  def put_topic_names(%{topics: topics} = response, resolver) when is_list(topics) do
    %{response | topics: SharedHelpers.put_topic_names(topics, resolver)}
  end

  def put_topic_names(response, _resolver), do: response

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
