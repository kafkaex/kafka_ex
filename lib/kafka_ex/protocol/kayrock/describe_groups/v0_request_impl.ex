defimpl KafkaEx.Protocol.Kayrock.DescribeGroups.Request, for: Kayrock.DescribeGroups.V0.Request do
  @moduledoc """
  Implementation for DescribeGroups v0 Request.

  V0 is the basic DescribeGroups request: `groups` (array of group IDs).
  """

  alias KafkaEx.Protocol.Kayrock.DescribeGroups.RequestHelpers

  def build_request(request_template, opts) do
    RequestHelpers.build_request_from_template(request_template, opts)
  end
end
