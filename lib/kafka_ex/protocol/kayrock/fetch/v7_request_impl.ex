defimpl KafkaEx.Protocol.Kayrock.Fetch.Request, for: Kayrock.Fetch.V7.Request do
  @moduledoc """
  Implementation for Fetch V7 Request.

  V7 adds incremental fetch session support:
  - `session_id` - Fetch session ID (0 for no session)
  - `session_epoch` - Session epoch (-1 for initial fetch)
  - `forgotten_topics_data` - Topics/partitions to remove from session

  Also includes V3+ max_bytes, V4+ isolation_level, V5+ log_start_offset.
  """

  alias KafkaEx.Protocol.Kayrock.Fetch.RequestHelpers

  def build_request(request, opts) do
    RequestHelpers.build_request_v7_plus(request, opts, 7)
  end
end
