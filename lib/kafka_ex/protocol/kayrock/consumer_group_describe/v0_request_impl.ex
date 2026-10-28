defimpl KafkaEx.Protocol.Kayrock.ConsumerGroupDescribe.Request, for: Kayrock.ConsumerGroupDescribe.V0.Request do
  @moduledoc """
  Implementation for ConsumerGroupDescribe v0 Request.

  V0 is the initial KIP-848 version.
  """

  alias KafkaEx.Protocol.Kayrock.ConsumerGroupDescribe.RequestHelpers

  def build_request(request_template, opts) do
    RequestHelpers.build_request_from_template(request_template, opts)
  end
end
