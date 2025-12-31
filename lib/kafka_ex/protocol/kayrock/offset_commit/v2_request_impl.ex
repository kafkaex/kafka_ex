defimpl KafkaEx.Protocol.Kayrock.OffsetCommit.Request, for: Kayrock.OffsetCommit.V2.Request do
  @moduledoc """
  Implementation for OffsetCommit v2 Request.

  This version stores offsets in Kafka (not Zookeeper) and:
  - Removes the timestamp field (Kafka manages timestamps)
  - Adds retention_time field for offset expiration
  This is the recommended version for modern Kafka deployments.
  """

  alias KafkaEx.Protocol.Kayrock.OffsetCommit.RequestHelpers

  def build_request(request_template, opts) do
    RequestHelpers.build_v2_v3_request(request_template, opts)
  end
end
