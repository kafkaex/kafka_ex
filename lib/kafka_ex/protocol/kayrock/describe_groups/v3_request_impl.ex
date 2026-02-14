defimpl KafkaEx.Protocol.Kayrock.DescribeGroups.Request, for: Kayrock.DescribeGroups.V3.Request do
  @moduledoc """
  Implementation for DescribeGroups v3 Request.

  V3 adds `include_authorized_operations` boolean (KIP-430).
  Response adds `authorized_operations` per group.
  """

  alias KafkaEx.Protocol.Kayrock.DescribeGroups.RequestHelpers

  def build_request(request_template, opts) do
    RequestHelpers.build_v3_plus_request(request_template, opts)
  end
end
