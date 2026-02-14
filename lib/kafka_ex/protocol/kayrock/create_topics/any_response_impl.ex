defimpl KafkaEx.Protocol.Kayrock.CreateTopics.Response, for: Any do
  @moduledoc """
  Fallback implementation of CreateTopics Response protocol for unknown future versions.

  All known Kayrock-supported versions (V0-V5) have explicit protocol
  implementations. This `Any` fallback exists solely for forward compatibility
  with potential future Kayrock versions that don't yet have an explicit impl.

  Detection logic based on response struct fields:
  - Has `throttle_time_ms` key and first topic has `num_partitions`: V5+ path
  - Has `throttle_time_ms` key: V2+ path (also covers V3, V4)
  - First topic has `error_message` key: V1 path
  - Otherwise: V0 path
  """

  alias KafkaEx.Protocol.Kayrock.CreateTopics.ResponseHelpers

  def parse_response(response) do
    topics = Map.get(response, :topics, [])

    cond do
      Map.has_key?(response, :throttle_time_ms) and has_v5_fields?(topics) ->
        # V5+ path: parse with extra fields
        topic_results = ResponseHelpers.parse_v5_topic_results(topics)
        {:ok, ResponseHelpers.build_response(topic_results, response.throttle_time_ms)}

      Map.has_key?(response, :throttle_time_ms) ->
        # V2/V3/V4 path: has throttle + error_message
        topic_results = ResponseHelpers.parse_topic_results(topics, true)
        {:ok, ResponseHelpers.build_response(topic_results, response.throttle_time_ms)}

      first_topic_has_error_message?(topics) ->
        # V1 path: has error_message but no throttle
        topic_results = ResponseHelpers.parse_topic_results(topics, true)
        {:ok, ResponseHelpers.build_response(topic_results)}

      true ->
        # V0 path: no error_message, no throttle
        topic_results = ResponseHelpers.parse_topic_results(topics, false)
        {:ok, ResponseHelpers.build_response(topic_results)}
    end
  end

  defp has_v5_fields?([first_topic | _]) do
    Map.has_key?(first_topic, :num_partitions)
  end

  defp has_v5_fields?(_), do: false

  defp first_topic_has_error_message?([first_topic | _]) do
    Map.has_key?(first_topic, :error_message)
  end

  defp first_topic_has_error_message?(_), do: false
end
