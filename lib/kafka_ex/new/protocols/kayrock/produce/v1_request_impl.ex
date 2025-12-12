defimpl KafkaEx.New.Protocols.Kayrock.Produce.Request, for: Kayrock.Produce.V1.Request do
  @moduledoc """
  Implementation for Produce V1 Request.

  V1 has the same structure as V0, using MessageSet format.
  V1 clarified acks semantics but the request format is identical.
  """

  alias KafkaEx.New.Protocols.Kayrock.Produce.RequestHelpers

  def build_request(request_template, opts) do
    RequestHelpers.build_request_v0_v2(request_template, opts)
  end
end
