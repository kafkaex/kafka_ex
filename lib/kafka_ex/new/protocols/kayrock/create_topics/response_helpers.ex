defmodule KafkaEx.New.Protocols.Kayrock.CreateTopics.ResponseHelpers do
  @moduledoc """
  Shared helper functions for parsing CreateTopics responses across all versions.
  """

  alias KafkaEx.New.Kafka.CreateTopics
  alias KafkaEx.New.Kafka.CreateTopics.TopicResult
  alias Kayrock.ErrorCode

  @doc """
  Parses topic errors from Kayrock response format to TopicResult structs.
  """
  @spec parse_topic_results(list(), boolean()) :: [TopicResult.t()]
  def parse_topic_results(topic_errors, has_error_message?) do
    Enum.map(topic_errors, fn topic_error ->
      error = ErrorCode.code_to_atom(topic_error.error_code)

      error_message =
        if has_error_message? do
          Map.get(topic_error, :error_message)
        else
          nil
        end

      TopicResult.build(
        topic: topic_error.topic,
        error: error,
        error_message: error_message
      )
    end)
  end

  @doc """
  Builds CreateTopics struct from parsed topic results.
  """
  @spec build_response([TopicResult.t()], non_neg_integer() | nil) :: CreateTopics.t()
  def build_response(topic_results, throttle_time_ms \\ nil) do
    CreateTopics.build(
      topic_results: topic_results,
      throttle_time_ms: throttle_time_ms
    )
  end
end
