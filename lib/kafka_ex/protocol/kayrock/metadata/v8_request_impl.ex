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

  def build_request(request_template, opts) do
    RequestHelpers.build_v8_plus_request(request_template, opts)
  end
end
