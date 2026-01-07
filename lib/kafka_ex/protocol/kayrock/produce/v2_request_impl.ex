defimpl KafkaEx.Protocol.Kayrock.Produce.Request, for: Kayrock.Produce.V2.Request do
  @moduledoc """
  Implementation for Produce V2 Request.

  V2 uses MessageSet format like V0/V1, but adds timestamp support
  at the message level. The request structure remains the same.
  """

  alias KafkaEx.Protocol.Kayrock.Produce.RequestHelpers

  def build_request(request_template, opts) do
    RequestHelpers.build_request_v0_v2(request_template, opts)
  end
end
