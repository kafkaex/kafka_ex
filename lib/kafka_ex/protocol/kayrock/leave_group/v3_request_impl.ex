defimpl KafkaEx.Protocol.Kayrock.LeaveGroup.Request, for: Kayrock.LeaveGroup.V3.Request do
  @moduledoc """
  Implementation for LeaveGroup v3 Request.

  V3 introduces a **structural change** (KIP-345 batch leave):
  the single `member_id` field is replaced with a `members` array.
  Each member has `member_id` and `group_instance_id`.
  """

  alias KafkaEx.Protocol.Kayrock.LeaveGroup.RequestHelpers

  def build_request(request_template, opts) do
    RequestHelpers.build_v3_plus_request(request_template, opts)
  end
end
