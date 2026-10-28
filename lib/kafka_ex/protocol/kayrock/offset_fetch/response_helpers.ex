defmodule KafkaEx.Protocol.Kayrock.OffsetFetch.ResponseHelpers do
  @moduledoc """
  Shared helper functions for parsing OffsetFetch responses across all versions.

  Version differences:
  - V0/V1: No top-level error_code field
  - V2+: Includes top-level error_code for broker-level errors
  - V5+: Adds `committed_leader_epoch` per partition (extracted dynamically)
  - V6: Flexible version (compact encodings, tagged fields -- handled by Kayrock)
  - V7: Same response as V6 (may return `UNSTABLE_OFFSET_COMMIT` per partition)
  - V8+: Results nested under a batched `groups` array, with a per-group
    `error_code` replacing the top-level one
  - V10: Topics identified by `topic_id`; names are attached first with
    `put_topic_names/2`
  """

  import KafkaEx.Protocol.Kayrock.ResponseHelpers,
    only: [build_response: 1, check_topic_names: 1, fail_fast_iterate_topics: 2, fail_fast_iterate_partitions: 3]

  alias KafkaEx.Protocol.Kayrock.ResponseHelpers, as: SharedHelpers

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
  Parses an OffsetFetch response with top-level error_code (V2+).

  Handles all V2+ versions uniformly. When `committed_leader_epoch` is present
  in partition data (V5+), it is extracted into `leader_epoch`. When absent
  (V2-V4), `leader_epoch` defaults to nil.
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

  @doc """
  Parses an OffsetFetch V8+ response (batched `groups`).

  KafkaEx always fetches for exactly one group, so the response is expected to
  hold one group entry. A non-zero group-level `error_code` is returned as
  `{:error, %KafkaEx.Client.Error{}}` (the V2-V7 top-level error moved here);
  otherwise the group's topics are parsed exactly as for earlier versions.
  """
  @spec parse_grouped_response(map()) :: {:ok, list(Offset.t())} | {:error, any()}
  def parse_grouped_response(%{groups: [_ | _] = groups}) do
    Enum.reduce_while(groups, {:ok, []}, fn group, {:ok, acc} ->
      case parse_group(group) do
        {:ok, offsets} -> {:cont, {:ok, acc ++ offsets}}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  def parse_grouped_response(%{groups: _}) do
    {:error, ErrorStruct.build(:unknown_server_error, %{reason: :empty_groups})}
  end

  @doc """
  Parses an OffsetFetch V10+ response: like `parse_grouped_response/1`, but
  every topic must have had its `name` attached from its `topic_id`
  (`put_topic_names/2`). An unresolved id yields `:unknown_topic_id`.
  """
  @spec parse_topic_id_grouped_response(map()) :: {:ok, list(Offset.t())} | {:error, any()}
  def parse_topic_id_grouped_response(%{groups: groups} = response) do
    with :ok <- check_topic_names(Enum.flat_map(groups, &(Map.get(&1, :topics) || []))) do
      parse_grouped_response(response)
    end
  end

  @doc """
  Attaches topic names to the id-only topics of an OffsetFetch V10+ response.
  A no-op for earlier versions, whose topics already carry names.
  """
  @spec put_topic_names(map(), SharedHelpers.topic_name_resolver()) :: map()
  def put_topic_names(%{groups: groups} = response, resolver) when is_list(groups) do
    groups =
      Enum.map(groups, fn group ->
        Map.update(group, :topics, [], &SharedHelpers.put_topic_names(&1 || [], resolver))
      end)

    %{response | groups: groups}
  end

  def put_topic_names(response, _resolver), do: response

  # ------------------------------------------------------------------------------
  # Private Functions
  # ------------------------------------------------------------------------------

  defp parse_group(%{error_code: error_code}) when error_code != 0 do
    {:error, ErrorStruct.build(error_code, %{})}
  end

  defp parse_group(%{topics: topics}) do
    (topics || [])
    |> fail_fast_iterate_topics(&parse_partition_responses/2)
    |> build_response()
  end

  defp parse_partition_responses(topic, partition_responses) do
    fail_fast_iterate_partitions(partition_responses, topic, &build_offset/2)
  end

  defp build_offset(
         topic,
         %{partition_index: partition, error_code: 0, committed_offset: offset, metadata: metadata} = partition_resp
       ) do
    data = %{
      partition: partition,
      offset: offset,
      error_code: :no_error,
      metadata: metadata || "",
      leader_epoch: Map.get(partition_resp, :committed_leader_epoch)
    }

    {:ok, Offset.from_list_offset(topic, [data])}
  end

  defp build_offset(topic, %{error_code: error_code, partition_index: partition}) do
    {:error, {error_code, topic, partition}}
  end
end
