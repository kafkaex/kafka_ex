defimpl KafkaEx.Protocol.Kayrock.DescribeGroups.Response,
  for: Kayrock.DescribeGroups.V2.Response do
  @moduledoc """
  Implementation for DescribeGroups v2 Response.

  V2 is a pure version bump. Response schema identical to V1.
  """

  alias KafkaEx.Protocol.Kayrock.DescribeGroups.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_response(response)
  end
end
