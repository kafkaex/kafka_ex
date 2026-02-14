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

  def build_request(request_template, opts) do
    RequestHelpers.build_v8_plus_request(request_template, opts)
  end
end
