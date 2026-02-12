defimpl KafkaEx.Protocol.Kayrock.Metadata.Request, for: Kayrock.Metadata.V8.Request do
  @moduledoc """
  Implementation of Metadata Request protocol for Kafka V8 API.

  V8 adds two new request fields:
  - `include_cluster_authorized_operations` - Whether to include cluster
    authorized operations in the response (default: false)
  - `include_topic_authorized_operations` - Whether to include topic
    authorized operations in the response (default: false)

  The response adds `cluster_authorized_operations` and per-topic
  `topic_authorized_operations` fields.
  """

  alias KafkaEx.Protocol.Kayrock.Metadata.RequestHelpers

  @doc """
  Builds a V8 Metadata request.

  ## Options

  - `:topics` - List of topic names to fetch metadata for, or `nil`/`[]` for all topics
  - `:allow_auto_topic_creation` - Whether to auto-create topics (default: false)
  - `:include_cluster_authorized_operations` - Include cluster auth ops (default: false)
  - `:include_topic_authorized_operations` - Include topic auth ops (default: false)
  """
  def build_request(request_template, opts) do
    topics = RequestHelpers.build_topics_list(opts)
    allow_auto = RequestHelpers.allow_auto_topic_creation?(opts)
    include_cluster_ops = Keyword.get(opts, :include_cluster_authorized_operations, false)
    include_topic_ops = Keyword.get(opts, :include_topic_authorized_operations, false)

    request_template
    |> Map.put(:topics, topics)
    |> Map.put(:allow_auto_topic_creation, allow_auto)
    |> Map.put(:include_cluster_authorized_operations, include_cluster_ops)
    |> Map.put(:include_topic_authorized_operations, include_topic_ops)
  end
end
