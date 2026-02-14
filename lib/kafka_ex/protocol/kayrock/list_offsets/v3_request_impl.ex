defimpl KafkaEx.Protocol.Kayrock.ListOffsets.Request, for: Kayrock.ListOffsets.V3.Request do
  @moduledoc """
  Implementation for ListOffsets V3 Request.

  V3 request schema in Kayrock is identical to V2:
  - `replica_id`, `isolation_level`
  - `topics` with partitions containing `partition` and `timestamp`

  Note: The Kafka protocol spec introduces `current_leader_epoch` in V3,
  but Kayrock's V3 schema does not include it (it appears in V4+).
  This means V3 requests via Kayrock behave identically to V2.
  """

  alias KafkaEx.Protocol.Kayrock.ListOffsets.RequestHelpers

  def build_request(request, opts) do
    RequestHelpers.build_request_v2_plus(request, opts, 3)
  end
end
