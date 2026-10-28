defimpl KafkaEx.Protocol.Kayrock.OffsetFetch.Request, for: Kayrock.OffsetFetch.V7.Request do
  @moduledoc """
  Implementation of OffsetFetch Request protocol for API version 7.

  V7 adds `require_stable` (KIP-447, defaults to `false` from the
  `:require_stable` option). Topics are otherwise built as in V6.
  """

  alias KafkaEx.Protocol.Kayrock.OffsetFetch.RequestHelpers

  def build_request(request_template, opts) do
    RequestHelpers.build_v7_request(request_template, opts)
  end
end
