defimpl KafkaEx.Protocol.Kayrock.DescribeGroups.Request, for: Kayrock.DescribeGroups.V5.Request do
  @moduledoc """
  Implementation for DescribeGroups v5 Request.

  V5 is the flexible version (KIP-482) with compact encoding + tagged_fields.
  Domain-relevant fields are identical to V3/V4. Kayrock handles compact encoding.
  """

  alias KafkaEx.Protocol.Kayrock.DescribeGroups.RequestHelpers

  def build_request(request_template, opts) do
    RequestHelpers.build_v3_plus_request(request_template, opts)
  end
end
