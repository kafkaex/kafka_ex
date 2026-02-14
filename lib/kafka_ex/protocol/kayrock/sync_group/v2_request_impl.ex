defimpl KafkaEx.Protocol.Kayrock.SyncGroup.Request, for: Kayrock.SyncGroup.V2.Request do
  @moduledoc """
  Implementation for SyncGroup v2 Request.

  V2 request has the same structure as V0/V1 -- the difference is purely
  a version bump for Kafka compatibility.
  """

  alias KafkaEx.Protocol.Kayrock.SyncGroup.RequestHelpers

  def build_request(request_template, opts) do
    RequestHelpers.build_request_from_template(request_template, opts)
  end
end
