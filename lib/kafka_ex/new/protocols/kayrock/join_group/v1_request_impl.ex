defimpl KafkaEx.New.Protocols.Kayrock.JoinGroup.Request, for: Kayrock.JoinGroup.V1.Request do
  @moduledoc """
  Implementation for JoinGroup v1 Request.

  V1 adds rebalance_timeout compared to v0.
  """

  alias KafkaEx.New.Protocols.Kayrock.JoinGroup.RequestHelpers

  def build_request(request_template, opts) do
    RequestHelpers.build_v1_or_v2_request(request_template, opts)
  end
end
