defimpl KafkaEx.Protocol.Kayrock.DescribeGroups.Response,
  for: Kayrock.DescribeGroups.V4.Response do
  @moduledoc """
  Implementation for DescribeGroups v4 Response.

  V4 response adds `group_instance_id: :nullable_string` per member (KIP-345).
  Also has `authorized_operations` per group (from V3).
  """

  alias KafkaEx.Protocol.Kayrock.DescribeGroups.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_response(response)
  end
end
