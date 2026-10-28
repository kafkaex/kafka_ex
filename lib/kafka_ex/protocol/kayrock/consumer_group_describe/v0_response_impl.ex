defimpl KafkaEx.Protocol.Kayrock.ConsumerGroupDescribe.Response, for: Kayrock.ConsumerGroupDescribe.V0.Response do
  @moduledoc """
  Implementation for ConsumerGroupDescribe v0 Response.

  V0 is the initial KIP-848 version; members have no `member_type`.
  """

  alias KafkaEx.Protocol.Kayrock.ConsumerGroupDescribe.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_response(response)
  end
end
