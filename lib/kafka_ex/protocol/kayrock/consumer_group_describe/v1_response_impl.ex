defimpl KafkaEx.Protocol.Kayrock.ConsumerGroupDescribe.Response, for: Kayrock.ConsumerGroupDescribe.V1.Response do
  @moduledoc """
  Implementation for ConsumerGroupDescribe v1 Response.

  V1 adds `member_type` per member (KIP-1099).
  """

  alias KafkaEx.Protocol.Kayrock.ConsumerGroupDescribe.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_response(response)
  end
end
