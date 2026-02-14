defimpl KafkaEx.Protocol.Kayrock.SyncGroup.Request, for: Kayrock.SyncGroup.V3.Request do
  @moduledoc """
  Implementation for SyncGroup v3 Request.

  V3 adds `group_instance_id` for static membership (KIP-345).
  """

  alias KafkaEx.Protocol.Kayrock.SyncGroup.RequestHelpers

  def build_request(request_template, opts) do
    RequestHelpers.build_v3_plus_request(request_template, opts)
  end
end
