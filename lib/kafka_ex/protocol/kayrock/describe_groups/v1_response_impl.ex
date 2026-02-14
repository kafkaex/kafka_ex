defimpl KafkaEx.Protocol.Kayrock.DescribeGroups.Response,
  for: Kayrock.DescribeGroups.V1.Response do
  @moduledoc """
  Implementation for DescribeGroups v1 Response.

  V1 response adds `throttle_time_ms` (not exposed in domain layer).
  Groups schema identical to V0.
  """

  alias KafkaEx.Protocol.Kayrock.DescribeGroups.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_response(response)
  end
end
