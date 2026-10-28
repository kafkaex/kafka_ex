defimpl KafkaEx.Protocol.Kayrock.OffsetFetch.Request, for: Kayrock.OffsetFetch.V9.Request do
  @moduledoc """
  Implementation of OffsetFetch Request protocol for API version 9.

  V9 is the first version usable with the KIP-848 consumer protocol: each
  group adds `member_id` / `member_epoch`, taken from `:member_id` /
  `:member_epoch` (or `nil` / `-1` for classic and standalone fetches when no
  `:member_epoch` is given). Otherwise the same as V8.
  """

  alias KafkaEx.Protocol.Kayrock.OffsetFetch.RequestHelpers

  def build_request(request_template, opts) do
    RequestHelpers.build_v9_request(request_template, opts)
  end
end
