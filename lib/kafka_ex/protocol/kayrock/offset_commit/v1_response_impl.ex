defimpl KafkaEx.Protocol.Kayrock.OffsetCommit.Response, for: Kayrock.OffsetCommit.V1.Response do
  @moduledoc """
  Implementation for OffsetCommit V1 Response.

  V1 uses Kafka/Zookeeper-based offset commit with consumer group coordination.
  Response only contains partition and error_code (no offset returned).
  """

  alias KafkaEx.Protocol.Kayrock.OffsetCommit.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_response(response)
  end
end
