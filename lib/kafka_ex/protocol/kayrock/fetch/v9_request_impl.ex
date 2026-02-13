defimpl KafkaEx.Protocol.Kayrock.Fetch.Request, for: Kayrock.Fetch.V9.Request do
  @moduledoc """
  Implementation for Fetch V9 Request.

  V9 adds `current_leader_epoch` to each partition in the fetch request.
  This allows the broker to detect stale fetch requests from consumers
  that have not yet learned about a leader change, enabling fenced
  fetch behavior.

  The default value of -1 means "no epoch specified" (unknown).

  All other fields remain the same as V7/V8.
  """

  alias KafkaEx.Protocol.Kayrock.Fetch.RequestHelpers

  def build_request(request, opts) do
    RequestHelpers.build_request_v7_plus(request, opts, 9)
  end
end
