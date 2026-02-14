defimpl KafkaEx.Protocol.Kayrock.DescribeGroups.Response,
  for: Kayrock.DescribeGroups.V3.Response do
  @moduledoc """
  Implementation for DescribeGroups v3 Response.

  V3 response adds `authorized_operations: :int32` per group (KIP-430).
  Members schema identical to V0-V2.
  """

  alias KafkaEx.Protocol.Kayrock.DescribeGroups.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_response(response)
  end
end
