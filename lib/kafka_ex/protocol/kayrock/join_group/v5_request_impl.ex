defimpl KafkaEx.Protocol.Kayrock.JoinGroup.Request, for: Kayrock.JoinGroup.V5.Request do
  @moduledoc """
  Implementation for JoinGroup V5 Request.

  V5 adds `group_instance_id` for static membership (KIP-345).
  When `group_instance_id` is set, the member retains its partition
  assignments across restarts without triggering a full rebalance.
  """

  alias KafkaEx.Protocol.Kayrock.JoinGroup.RequestHelpers

  def build_request(request_template, opts) do
    RequestHelpers.build_v5_plus_request(request_template, opts)
  end
end
