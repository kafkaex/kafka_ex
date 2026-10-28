defimpl KafkaEx.Protocol.Kayrock.OffsetCommit.Request, for: Kayrock.OffsetCommit.V9.Request do
  @moduledoc """
  Implementation of OffsetCommit Request protocol for API version 9.

  V9 is the first version usable with the KIP-848 consumer group protocol.
  The wire format is the same as V8, but `generation_id` is renamed to
  `generation_id_or_member_epoch`: the classic generation id (`:generation_id`)
  or, for consumer-protocol groups, the member epoch (`:member_epoch`, which
  wins when both are given).
  """

  alias KafkaEx.Protocol.Kayrock.OffsetCommit.RequestHelpers

  def build_request(request_template, opts) do
    RequestHelpers.build_v9_request(request_template, opts)
  end
end
