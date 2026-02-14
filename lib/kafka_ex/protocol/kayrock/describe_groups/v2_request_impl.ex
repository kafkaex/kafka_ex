defimpl KafkaEx.Protocol.Kayrock.DescribeGroups.Request, for: Kayrock.DescribeGroups.V2.Request do
  @moduledoc """
  Implementation for DescribeGroups v2 Request.

  V2 is a pure version bump. Request schema identical to V0/V1.
  """

  alias KafkaEx.Protocol.Kayrock.DescribeGroups.RequestHelpers

  def build_request(request_template, opts) do
    RequestHelpers.build_request_from_template(request_template, opts)
  end
end
