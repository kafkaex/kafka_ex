defmodule KafkaEx.Protocol.Kayrock.CreateTopics.ResponseHelpers do
  @moduledoc """
  Shared helper functions for parsing CreateTopics responses across all versions.
  """

  alias KafkaEx.Messages.CreateTopics
  alias KafkaEx.Messages.CreateTopics.TopicResult
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
        topic: topic_error.name,
        error: error,
        error_message: error_message
      )
    end)
  end

  @doc """
  Parses topic results from V5 Kayrock response format to TopicResult structs.

  V5 adds per-topic fields: `num_partitions`, `replication_factor`, and `configs`.
  Each config entry has: `name`, `value`, `read_only`, `config_source`, `is_sensitive`.
  """
  @spec parse_v5_topic_results(list()) :: [TopicResult.t()]
  def parse_v5_topic_results(topics) do
    Enum.map(topics, fn topic ->
      error = ErrorCode.code_to_atom(topic.error_code)

      configs =
        case Map.get(topic, :configs) do
          nil ->
            nil

          config_list when is_list(config_list) ->
            Enum.map(config_list, fn config ->
              %{
                name: config.name,
                value: Map.get(config, :value),
                read_only: Map.get(config, :read_only, false),
                config_source: Map.get(config, :config_source, -1),
                is_sensitive: Map.get(config, :is_sensitive, false)
              }
            end)
        end

      TopicResult.build(
        topic: topic.name,
        error: error,
        error_message: Map.get(topic, :error_message),
        num_partitions: Map.get(topic, :num_partitions),
        replication_factor: Map.get(topic, :replication_factor),
        configs: configs
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
