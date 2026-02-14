defimpl KafkaEx.Protocol.Kayrock.OffsetCommit.Request, for: Kayrock.OffsetCommit.V4.Request do
  @moduledoc """
  Implementation of OffsetCommit Request protocol for API version 4.

  V4 is a pure version bump -- request structure is identical to V3.
  Includes retention_time_ms, generation_id, member_id.
  """

  alias KafkaEx.Protocol.Kayrock.OffsetCommit.RequestHelpers

  def build_request(request_template, opts) do
    RequestHelpers.build_v2_v3_request(request_template, opts)
  end
end
