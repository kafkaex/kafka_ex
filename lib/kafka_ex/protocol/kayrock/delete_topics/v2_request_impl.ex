defimpl KafkaEx.Protocol.Kayrock.DeleteTopics.Request,
  for: Kayrock.DeleteTopics.V2.Request do
  @moduledoc """
  V2 implementation of DeleteTopics Request protocol.

  V2 Schema (identical to V0/V1):
  - topics: List of topic names to delete
  - timeout: Request timeout in milliseconds
  """

  alias KafkaEx.Protocol.Kayrock.DeleteTopics.RequestHelpers

  def build_request(request, opts) do
    RequestHelpers.build_request_from_template(request, opts)
  end
end
