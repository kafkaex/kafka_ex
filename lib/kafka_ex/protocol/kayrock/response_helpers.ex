defmodule KafkaEx.Protocol.Kayrock.ResponseHelpers do
  @moduledoc """
  Shared helper functions for parsing protocol responses.

  This module provides common functionality for handling responses across
  multiple protocol implementations (OffsetCommit, OffsetFetch, etc.).
  """

  alias KafkaEx.Client.Error, as: ErrorStruct

  @type error_tuple :: {atom(), String.t(), non_neg_integer()}
  @type parser_fn :: (String.t(), list() -> {:ok, any()} | {:error, error_tuple()})

  @doc """
  Builds a response tuple from either a list of offsets or an error.
  """
  @spec build_response(list() | error_tuple()) :: {:ok, list()} | {:error, ErrorStruct.t()}
  def build_response(offsets) when is_list(offsets), do: {:ok, offsets}

  def build_response({error_code, topic, partition}) do
    error = ErrorStruct.build(error_code, %{topic: topic, partition: partition})
    {:error, error}
  end

  @doc """
  Iterates over topics data with fail-fast behavior.

  Uses `name` and `partitions` keys by default (for OffsetCommit/OffsetFetch).
  Pass custom keys for APIs with different field names (e.g., ListOffsets uses `topic`/`partition_responses`).
  """
  @spec fail_fast_iterate_topics(list(), parser_fn(), Keyword.t()) :: list() | error_tuple()
  def fail_fast_iterate_topics(topics_data, parser_fn, opts \\ []) do
    topic_key = Keyword.get(opts, :topic_key, :name)
    partitions_key = Keyword.get(opts, :partitions_key, :partitions)

    Enum.reduce_while(topics_data, [], fn response, acc ->
      case parser_fn.(Map.fetch!(response, topic_key), Map.fetch!(response, partitions_key)) do
        {:ok, result} -> {:cont, merge_acc(result, acc)}
        {:error, error_data} -> {:halt, error_data}
      end
    end)
  end

  @doc """
  Iterates over partitions data with fail-fast behavior.
  """
  @spec fail_fast_iterate_partitions(list(), String.t(), parser_fn()) ::
          {:ok, list()} | {:error, error_tuple()}
  def fail_fast_iterate_partitions(partitions_data, topic, parser_fn) do
    partitions_data
    |> Enum.reduce_while([], fn datum, acc ->
      case parser_fn.(topic, datum) do
        {:ok, value} -> {:cont, merge_acc(value, acc)}
        {:error, error_data} -> {:halt, error_data}
      end
    end)
    |> case do
      results when is_list(results) -> {:ok, results}
      error_data -> {:error, error_data}
    end
  end

  @typedoc "Resolves a topic id to its name, e.g. via `KafkaEx.Cluster.ClusterMetadata.topic_name_by_id/2`."
  @type topic_name_resolver :: (binary() -> {:ok, String.t()} | {:error, term()})

  @doc """
  Attaches topic names to topics identified only by `topic_id` (OffsetCommit
  V10+, OffsetFetch V10+), using `resolver` (typically backed by the client's
  cluster metadata).

  Topics that already have a binary `name` are left unchanged, as are topics
  whose id the resolver does not know, so the version-specific parser can
  report `:unknown_topic_id`. The resolver is only called for id-only topics.
  """
  @spec put_topic_names([map()], topic_name_resolver()) :: [map()]
  def put_topic_names(topics, resolver) when is_list(topics) and is_function(resolver, 1) do
    Enum.map(topics, fn
      %{name: name} = topic when is_binary(name) ->
        topic

      %{topic_id: topic_id} = topic ->
        case resolver.(topic_id) do
          {:ok, name} -> Map.put(topic, :name, name)
          _ -> topic
        end

      topic ->
        topic
    end)
  end

  @doc """
  Returns `{:error, %Error{error: :unknown_topic_id}}` for the first topic with
  no resolved `name`, or `:ok` when every topic has one.
  """
  @spec check_topic_names([map()]) :: :ok | {:error, ErrorStruct.t()}
  def check_topic_names(topics) do
    case Enum.find(topics, &(not is_binary(Map.get(&1, :name)))) do
      nil -> :ok
      topic -> {:error, ErrorStruct.build(:unknown_topic_id, %{topic_id: Map.get(topic, :topic_id)})}
    end
  end

  @doc """
  Parses time values for list_offsets requests.
  """
  @spec parse_time(:latest | :earliest | integer() | DateTime.t()) :: integer()
  def parse_time(:latest), do: -1
  def parse_time(:earliest), do: -2
  def parse_time(time) when is_integer(time), do: time
  def parse_time(%DateTime{} = time), do: DateTime.to_unix(time, :millisecond)

  # ------------------------------------------------------------------------------
  # Private Functions
  # ------------------------------------------------------------------------------

  defp merge_acc(result, acc) when is_list(result), do: result ++ acc
  defp merge_acc(result, acc), do: [result | acc]
end
