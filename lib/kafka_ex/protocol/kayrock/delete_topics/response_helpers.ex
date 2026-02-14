defmodule KafkaEx.Protocol.Kayrock.DeleteTopics.ResponseHelpers do
  @moduledoc """
  Shared helper functions for parsing DeleteTopics responses across all versions.
  """

  alias KafkaEx.Messages.DeleteTopics
  alias KafkaEx.Messages.DeleteTopics.TopicResult
  alias Kayrock.ErrorCode

  @doc """
  Parses topic error codes from Kayrock response format to TopicResult structs.
  """
  @spec parse_topic_results(list()) :: [TopicResult.t()]
  def parse_topic_results(topic_error_codes) do
    Enum.map(topic_error_codes, fn topic_error ->
      error = ErrorCode.code_to_atom(topic_error.error_code)

      TopicResult.build(
        topic: topic_error.name,
        error: error
      )
    end)
  end

  @doc """
  Builds DeleteTopics struct from parsed topic results.
  """
  @spec build_response([TopicResult.t()], non_neg_integer() | nil) :: DeleteTopics.t()
  def build_response(topic_results, throttle_time_ms \\ nil) do
    DeleteTopics.build(
      topic_results: topic_results,
      throttle_time_ms: throttle_time_ms
    )
  end

  @doc """
  Parses a V0 DeleteTopics response (no throttle_time_ms).
  """
  @spec parse_v0_response(map()) :: {:ok, DeleteTopics.t()}
  def parse_v0_response(response) do
    topic_results = parse_topic_results(response.responses)
    {:ok, build_response(topic_results)}
  end

  @doc """
  Parses a V1+ DeleteTopics response (includes throttle_time_ms).
  """
  @spec parse_v1_plus_response(map()) :: {:ok, DeleteTopics.t()}
  def parse_v1_plus_response(response) do
    topic_results = parse_topic_results(response.responses)
    {:ok, build_response(topic_results, response.throttle_time_ms)}
  end
end
