defimpl KafkaEx.Protocol.Kayrock.OffsetCommit.Request, for: Kayrock.OffsetCommit.V7.Request do
  @moduledoc """
  Implementation of OffsetCommit Request protocol for API version 7.

  V7 adds group_instance_id for static group membership (KIP-345).
  Also includes committed_leader_epoch per partition (from V6).
  """

  alias KafkaEx.Protocol.Kayrock.OffsetCommit.RequestHelpers

  def build_request(request_template, opts) do
    RequestHelpers.build_v7_plus_request(request_template, opts)
  end
end
