defimpl KafkaEx.Protocol.Kayrock.DeleteTopics.Request,
  for: Kayrock.DeleteTopics.V1.Request do
  @moduledoc """
  V1 implementation of DeleteTopics Request protocol.

  V1 Schema (same as V0):
  - topics: List of topic names to delete
  - timeout: Request timeout in milliseconds

  V1 adds throttle_time_ms to the response, but request format is identical to V0.
  """

  alias KafkaEx.Protocol.Kayrock.DeleteTopics.RequestHelpers

  def build_request(request, opts) do
    fields = RequestHelpers.extract_common_fields(opts)

    %{
      request
      | topics: fields.topics,
        timeout: fields.timeout
    }
  end
end
