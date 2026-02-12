defimpl KafkaEx.Protocol.Kayrock.Metadata.Request, for: Kayrock.Metadata.V9.Request do
  @moduledoc """
  Implementation of Metadata Request protocol for Kafka V9 API.

  V9 is a flexible version (KIP-482) that introduces:
  - Compact array/string encodings for topics
  - `tagged_fields` at top-level and within topics array entries
  - Same logical fields as V8: topics, allow_auto_topic_creation,
    include_cluster_authorized_operations, include_topic_authorized_operations

  Tagged fields from the request are preserved as-is; KafkaEx does not
  currently populate any defined tagged field extensions.
  """

  alias KafkaEx.Protocol.Kayrock.Metadata.RequestHelpers

  @doc """
  Builds a V9 Metadata request.

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
