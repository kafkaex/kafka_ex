defimpl KafkaEx.Protocol.Kayrock.Fetch.Request, for: Kayrock.Fetch.V11.Request do
  @moduledoc """
  Implementation for Fetch V11 Request.

  V11 adds `rack_id` at the top level of the request, enabling rack-aware
  fetch. When specified, the broker can prefer replicas that are on the
  same rack as the consumer, reducing cross-rack network traffic.

  The default value is an empty string (no rack specified).

  All other fields remain the same as V9/V10, including
  `current_leader_epoch` in partition requests.
  """

  alias KafkaEx.Protocol.Kayrock.Fetch.RequestHelpers

  def build_request(request, opts) do
    RequestHelpers.build_request_v7_plus(request, opts, 11)
  end
end
