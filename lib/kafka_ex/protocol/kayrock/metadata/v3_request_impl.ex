defimpl KafkaEx.Protocol.Kayrock.Metadata.Request, for: Kayrock.Metadata.V3.Request do
  @moduledoc """
  Implementation of Metadata Request protocol for Kafka V3 API.

  V3 adds `throttle_time_ms` to the response but the request schema
  is identical to V1/V2 (topics array, no `allow_auto_topic_creation`).
  """

  alias KafkaEx.Protocol.Kayrock.Metadata.RequestHelpers

  @doc """
  Builds a V3 Metadata request.

  ## Options

  - `:topics` - List of topic names to fetch metadata for, or `nil`/`[]` for all topics
  """
  def build_request(request_template, opts) do
    topics = RequestHelpers.build_topics_list(opts)
    Map.put(request_template, :topics, topics)
  end
end
