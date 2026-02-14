defimpl KafkaEx.Protocol.Kayrock.ListOffsets.Request, for: Kayrock.ListOffsets.V4.Request do
  @moduledoc """
  Implementation for ListOffsets V4 Request.

  V4 adds `current_leader_epoch` to each partition in the request.
  This allows the broker to detect stale offset requests from consumers
  that have not yet learned about a leader change, enabling fenced
  offset query behavior.

  The default value of -1 means "no epoch specified" (unknown).

  All other fields remain the same as V2/V3:
  - `replica_id`, `isolation_level`
  - `topics` with partitions containing `partition`, `current_leader_epoch`, `timestamp`
  """

  alias KafkaEx.Protocol.Kayrock.ListOffsets.RequestHelpers

  def build_request(request, opts) do
    RequestHelpers.build_request_v2_plus(request, opts, 4)
  end
end
