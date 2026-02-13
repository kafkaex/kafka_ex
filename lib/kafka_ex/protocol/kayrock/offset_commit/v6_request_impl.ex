defimpl KafkaEx.Protocol.Kayrock.OffsetCommit.Request, for: Kayrock.OffsetCommit.V6.Request do
  @moduledoc """
  Implementation of OffsetCommit Request protocol for API version 6.

  V6 adds committed_leader_epoch per partition. No retention_time_ms.
  """

  alias KafkaEx.Protocol.Kayrock.OffsetCommit.RequestHelpers

  def build_request(request_template, opts) do
    RequestHelpers.build_v6_request(request_template, opts)
  end
end
