defimpl KafkaEx.Protocol.Kayrock.Metadata.Request, for: Kayrock.Metadata.V13.Request do
  @moduledoc """
  Implementation of Metadata Request protocol for Kafka V13 API.

  The V13 request is identical to V12 (named topics and/or `:topic_ids`
  lookups); only the response changes.
  """

  alias KafkaEx.Protocol.Kayrock.Metadata.RequestHelpers

  def build_request(request_template, opts) do
    RequestHelpers.build_v12_plus_request(request_template, opts)
  end
end
