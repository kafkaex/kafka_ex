defimpl KafkaEx.New.Protocols.Kayrock.SyncGroup.Request, for: Kayrock.SyncGroup.V1.Request do
  @moduledoc """
  Implementation for SyncGroup v1 Request.

  V1 request has the same structure as v0 - the difference is in the response
  which includes throttle_time_ms.
  """

  alias KafkaEx.New.Protocols.Kayrock.SyncGroup.RequestHelpers

  def build_request(request_template, opts) do
    RequestHelpers.build_request_from_template(request_template, opts)
  end
end
