defimpl KafkaEx.Protocol.Kayrock.ConsumerGroupDescribe.Request, for: Kayrock.ConsumerGroupDescribe.V1.Request do
  @moduledoc """
  Implementation for ConsumerGroupDescribe v1 Request.

  V1 request schema is identical to V0.
  """

  alias KafkaEx.Protocol.Kayrock.ConsumerGroupDescribe.RequestHelpers

  def build_request(request_template, opts) do
    RequestHelpers.build_request_from_template(request_template, opts)
  end
end
