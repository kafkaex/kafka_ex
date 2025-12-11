defmodule KafkaEx.New.Protocols.Kayrock.ResponseHelpers do
  @moduledoc """
  Shared helper functions for parsing protocol responses.

  This module provides common functionality for handling responses across
  multiple protocol implementations (OffsetCommit, OffsetFetch, etc.).
  """

  alias KafkaEx.New.Client.Error, as: ErrorStruct

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
  """
  @spec fail_fast_iterate_topics(list(), parser_fn()) :: list() | error_tuple()
  def fail_fast_iterate_topics(topics_data, parser_fn) do
    Enum.reduce_while(topics_data, [], fn response, acc ->
      case parser_fn.(response.topic, response.partition_responses) do
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
