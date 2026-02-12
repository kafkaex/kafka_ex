defimpl KafkaEx.Protocol.Kayrock.Metadata.Request, for: Kayrock.Metadata.V4.Request do
  @moduledoc """
  Implementation of Metadata Request protocol for Kafka V4 API.

  V4 introduces `allow_auto_topic_creation` (boolean) which controls whether
  the broker should auto-create topics that don't exist when metadata is
  requested for them. Defaults to `false` for safety.
  """

  alias KafkaEx.Protocol.Kayrock.Metadata.RequestHelpers

  @doc """
  Builds a V4 Metadata request.

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
