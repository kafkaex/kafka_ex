defimpl KafkaEx.Protocol.Kayrock.DeleteTopics.Request,
  for: Kayrock.DeleteTopics.V3.Request do
  @moduledoc """
  V3 implementation of DeleteTopics Request protocol.

  V3 Schema (identical to V0-V2):
  - topics: List of topic names to delete
  - timeout: Request timeout in milliseconds
  """

  alias KafkaEx.Protocol.Kayrock.DeleteTopics.RequestHelpers

  def build_request(request, opts) do
    RequestHelpers.build_request_from_template(request, opts)
  end
end
