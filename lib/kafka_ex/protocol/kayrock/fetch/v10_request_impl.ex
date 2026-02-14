defimpl KafkaEx.Protocol.Kayrock.Fetch.Request, for: Kayrock.Fetch.V10.Request do
  @moduledoc """
  Implementation for Fetch V10 Request.

  V10 has the same request schema as V9:
  - Includes `current_leader_epoch` in partition requests
  - Supports incremental fetch sessions
  - Includes max_bytes, isolation_level, log_start_offset

  No request-side changes from V9.
  """

  alias KafkaEx.Protocol.Kayrock.Fetch.RequestHelpers

  def build_request(request, opts) do
    RequestHelpers.build_request_v7_plus(request, opts, 10)
  end
end
