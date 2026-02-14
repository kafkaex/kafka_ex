defimpl KafkaEx.Protocol.Kayrock.ListOffsets.Request, for: Kayrock.ListOffsets.V5.Request do
  @moduledoc """
  Implementation for ListOffsets V5 Request.

  V5 has the same request schema as V4:
  - `replica_id`, `isolation_level`
  - `topics` with partitions containing `partition`, `current_leader_epoch`, `timestamp`

  No request-side changes from V4.
  """

  alias KafkaEx.Protocol.Kayrock.ListOffsets.RequestHelpers

  def build_request(request, opts) do
    RequestHelpers.build_request_v2_plus(request, opts, 5)
  end
end
