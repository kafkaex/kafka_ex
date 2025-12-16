defimpl KafkaEx.New.Protocols.Kayrock.DeleteTopics.Request,
  for: Kayrock.DeleteTopics.V0.Request do
  @moduledoc """
  V0 implementation of DeleteTopics Request protocol.

  V0 Schema:
  - topics: List of topic names to delete
  - timeout: Request timeout in milliseconds
  """

  alias KafkaEx.New.Protocols.Kayrock.DeleteTopics.RequestHelpers

  def build_request(request, opts) do
    fields = RequestHelpers.extract_common_fields(opts)

    %{
      request
      | topics: fields.topics,
        timeout: fields.timeout
    }
  end
end
