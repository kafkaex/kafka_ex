defimpl KafkaEx.Protocol.Kayrock.OffsetCommit.Response, for: Kayrock.OffsetCommit.V0.Response do
  @moduledoc """
  Implementation for OffsetCommit V0 Response.

  V0 uses Zookeeper-based offset commit.
  Response only contains partition and error_code (no offset returned).
  """

  alias KafkaEx.Protocol.Kayrock.OffsetCommit.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_response(response)
  end
end
