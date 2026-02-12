defimpl KafkaEx.Protocol.Kayrock.Metadata.Request, for: Any do
  @moduledoc """
  Fallback implementation of Metadata Request protocol for Kafka V3+ APIs.

  Handles all standard fields including `allow_auto_topic_creation` (V4+).
  """

  alias KafkaEx.Protocol.Kayrock.Metadata.RequestHelpers

  @doc """
  Builds a Metadata request from options.

  Sets `:topics` on all versions and `:allow_auto_topic_creation` on V4+
  (when the struct has that field).
  """
  def build_request(request_template, opts) do
    topics = RequestHelpers.build_topics_list(opts)
    request = Map.put(request_template, :topics, topics)

    if Map.has_key?(request, :allow_auto_topic_creation) do
      Map.put(request, :allow_auto_topic_creation, RequestHelpers.allow_auto_topic_creation?(opts))
    else
      request
    end
  end
end
