defimpl KafkaEx.Protocol.Kayrock.Metadata.Request, for: Kayrock.Metadata.V6.Request do
  @moduledoc """
  Implementation of Metadata Request protocol for Kafka V6 API.

  V6 response is identical to V5 in schema. The request schema is identical
  to V4/V5: topics + allow_auto_topic_creation.
  """

  alias KafkaEx.Protocol.Kayrock.Metadata.RequestHelpers

  @doc """
  Builds a V6 Metadata request.

  ## Options

  - `:topics` - List of topic names to fetch metadata for, or `nil`/`[]` for all topics
  - `:allow_auto_topic_creation` - Whether to auto-create topics (default: false)
  """
  def build_request(request_template, opts) do
    topics = RequestHelpers.build_topics_list(opts)
    allow_auto = RequestHelpers.allow_auto_topic_creation?(opts)

    request_template
    |> Map.put(:topics, topics)
    |> Map.put(:allow_auto_topic_creation, allow_auto)
  end
end
