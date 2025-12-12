defimpl KafkaEx.New.Protocols.Kayrock.Produce.Request, for: Kayrock.Produce.V0.Request do
  @moduledoc """
  Implementation for Produce V0 Request.

  V0 is the basic Produce request using MessageSet format.
  Does not support timestamps or headers.
  """

  alias KafkaEx.New.Protocols.Kayrock.Produce.RequestHelpers

  def build_request(request_template, opts) do
    RequestHelpers.build_request_v0_v2(request_template, opts)
  end
end
