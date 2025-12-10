defimpl KafkaEx.New.Protocols.Kayrock.OffsetCommit.Request, for: Kayrock.OffsetCommit.V3.Request do
  @moduledoc """
  Implementation of OffsetCommit Request protocol for API version 3.

  V3 adds throttle_time_ms to the response.
  Request structure is identical to V2 (includes retention_time).
  """

  alias KafkaEx.New.Protocols.Kayrock.OffsetCommit.RequestHelpers

  def build_request(request_template, opts) do
    RequestHelpers.build_v2_v3_request(request_template, opts)
  end
end
