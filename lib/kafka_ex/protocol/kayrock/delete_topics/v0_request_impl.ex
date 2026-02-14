defimpl KafkaEx.Protocol.Kayrock.DeleteTopics.Request,
  for: Kayrock.DeleteTopics.V0.Request do
  @moduledoc """
  V0 implementation of DeleteTopics Request protocol.

  V0 Schema:
  - topics: List of topic names to delete
  - timeout: Request timeout in milliseconds
  """

  alias KafkaEx.Protocol.Kayrock.DeleteTopics.RequestHelpers

  def build_request(request, opts) do
    RequestHelpers.build_request_from_template(request, opts)
  end
end
