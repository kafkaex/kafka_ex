defimpl KafkaEx.Protocol.Kayrock.DescribeGroups.Request, for: Kayrock.DescribeGroups.V1.Request do
  @moduledoc """
  Implementation for DescribeGroups v1 Request.

  V1 request schema is identical to V0 (groups array only).
  V1 response adds throttle_time_ms.
  """

  alias KafkaEx.Protocol.Kayrock.DescribeGroups.RequestHelpers

  def build_request(request_template, opts) do
    RequestHelpers.build_request_from_template(request_template, opts)
  end
end
