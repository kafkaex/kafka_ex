defimpl KafkaEx.Protocol.Kayrock.DescribeGroups.Response,
  for: Kayrock.DescribeGroups.V5.Response do
  @moduledoc """
  Implementation for DescribeGroups v5 Response.

  V5 is the flexible version (KIP-482) with compact encoding + tagged_fields.
  Domain-relevant fields are identical to V4. Kayrock handles compact decoding.
  """

  alias KafkaEx.Protocol.Kayrock.DescribeGroups.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_response(response)
  end
end
