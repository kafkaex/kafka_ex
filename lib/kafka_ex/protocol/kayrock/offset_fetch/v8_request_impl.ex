defimpl KafkaEx.Protocol.Kayrock.OffsetFetch.Request, for: Kayrock.OffsetFetch.V8.Request do
  @moduledoc """
  Implementation of OffsetFetch Request protocol for API version 8.

  V8 batches groups (KIP-709): the single group KafkaEx fetches for is sent
  as one `groups` entry (`group_id` + `topics`). `require_stable` as in V7.
  """

  alias KafkaEx.Protocol.Kayrock.OffsetFetch.RequestHelpers

  def build_request(request_template, opts) do
    RequestHelpers.build_v8_request(request_template, opts)
  end
end
