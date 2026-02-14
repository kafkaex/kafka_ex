defimpl KafkaEx.Protocol.Kayrock.OffsetCommit.Request, for: Kayrock.OffsetCommit.V8.Request do
  @moduledoc """
  Implementation of OffsetCommit Request protocol for API version 8.

  V8 is the flexible version (KIP-482). Uses compact strings/arrays and
  tagged_fields. Kayrock handles the encoding; the request fields are
  identical to V7 (group_instance_id + committed_leader_epoch).
  """

  alias KafkaEx.Protocol.Kayrock.OffsetCommit.RequestHelpers

  def build_request(request_template, opts) do
    RequestHelpers.build_v7_plus_request(request_template, opts)
  end
end
