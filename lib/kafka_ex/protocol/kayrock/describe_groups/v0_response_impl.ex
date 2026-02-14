defimpl KafkaEx.Protocol.Kayrock.DescribeGroups.Response,
  for: Kayrock.DescribeGroups.V0.Response do
  @moduledoc """
  Implementation for DescribeGroups v0 Response.

  V0 response has groups array only (no throttle_time_ms).
  """

  alias KafkaEx.Protocol.Kayrock.DescribeGroups.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_response(response)
  end
end
