defimpl KafkaEx.Protocol.Kayrock.Metadata.Request, for: Any do
  @moduledoc """
  Fallback implementation of Metadata Request protocol for unknown future versions.

  All known Kayrock-supported versions (V0-V9) have explicit protocol
  implementations. This `Any` fallback exists solely for forward compatibility
  with potential future Kayrock versions that don't yet have an explicit impl.

  Handles all standard fields including `allow_auto_topic_creation` (V4+)
  and `include_*_authorized_operations` (V8+) when the struct has those fields.
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

    request =
      if Map.has_key?(request, :allow_auto_topic_creation) do
        Map.put(request, :allow_auto_topic_creation, RequestHelpers.allow_auto_topic_creation?(opts))
      else
        request
      end

    request =
      if Map.has_key?(request, :include_cluster_authorized_operations) do
        Map.put(
          request,
          :include_cluster_authorized_operations,
          Keyword.get(opts, :include_cluster_authorized_operations, false)
        )
      else
        request
      end

    if Map.has_key?(request, :include_topic_authorized_operations) do
      Map.put(
        request,
        :include_topic_authorized_operations,
        Keyword.get(opts, :include_topic_authorized_operations, false)
      )
    else
      request
    end
  end
end
