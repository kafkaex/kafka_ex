defmodule KafkaEx.New.Protocols.ListOffsets.Shared do
  @moduledoc false

  alias KafkaEx.New.Structs.Error, as: ErrorStruct

  # ------------------------------------------------------------------------------
  def build_response(offsets) when is_list(offsets), do: {:ok, offsets}

  def build_response({error_code, topic, partition}) do
    error = ErrorStruct.build(error_code, %{topic: topic, partition: partition})
    {:error, error}
  end

  def fail_fast_iterate_topics(topics_data, parser_fn) do
    Enum.reduce_while(topics_data, [], fn response, acc ->
      case parser_fn.(response.topic, response.partition_responses) do
        {:ok, result} -> {:cont, merge_acc(result, acc)}
        {:error, error_data} -> {:halt, error_data}
      end
    end)
  end

  # ------------------------------------------------------------------------------
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

  # --------------------------------------------------------------------------------
  def parse_time(:latest), do: -1
  def parse_time(:earliest), do: -2
  def parse_time(time) when is_integer(time), do: time
  def parse_time(%DateTime{} = time), do: DateTime.to_unix(time, :millisecond)

  # ------------------------------------------------------------------------------
  defp merge_acc(result, acc) when is_list(result), do: result ++ acc
  defp merge_acc(result, acc), do: [result | acc]
end
