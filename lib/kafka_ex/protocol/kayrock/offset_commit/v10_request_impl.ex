defimpl KafkaEx.Protocol.Kayrock.OffsetCommit.Request, for: Kayrock.OffsetCommit.V10.Request do
  @moduledoc """
  Implementation of OffsetCommit Request protocol for API version 10.

  V10 identifies topics by `topic_id` instead of name (KIP-848). Requires the
  `:topic_ids` option (`%{topic_name => <<_::128>>}`), which
  `KafkaEx.Client.RequestBuilder` fills from cluster metadata; when an id is
  not known the builder negotiates V9 instead. Otherwise the same as V9.
  """

  alias KafkaEx.Protocol.Kayrock.OffsetCommit.RequestHelpers

  def build_request(request_template, opts) do
    RequestHelpers.build_v10_request(request_template, opts)
  end
end
