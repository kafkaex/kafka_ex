defimpl KafkaEx.Protocol.Kayrock.DescribeGroups.Request, for: Kayrock.DescribeGroups.V4.Request do
  @moduledoc """
  Implementation for DescribeGroups v4 Request.

  V4 request schema is identical to V3 (groups + include_authorized_operations).
  V4 response adds `group_instance_id` per member (KIP-345).
  """

  alias KafkaEx.Protocol.Kayrock.DescribeGroups.RequestHelpers

  def build_request(request_template, opts) do
    RequestHelpers.build_v3_plus_request(request_template, opts)
  end
end
