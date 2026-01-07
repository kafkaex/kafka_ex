defimpl KafkaEx.Protocol.Kayrock.OffsetCommit.Response, for: Kayrock.OffsetCommit.V2.Response do
  @moduledoc """
  Implementation for OffsetCommit V2 Response.

  V2 uses Kafka-based offset commit (recommended).
  Response only contains partition and error_code (no offset returned).
  """

  alias KafkaEx.Protocol.Kayrock.OffsetCommit.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_response(response)
  end
end
