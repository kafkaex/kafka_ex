defimpl KafkaEx.Protocol.Kayrock.OffsetCommit.Request, for: Kayrock.OffsetCommit.V5.Request do
  @moduledoc """
  Implementation of OffsetCommit Request protocol for API version 5.

  V5 removes retention_time_ms from the request. Offset retention is now
  controlled exclusively by the broker's `offsets.retention.minutes` config.
  """

  alias KafkaEx.Protocol.Kayrock.OffsetCommit.RequestHelpers

  def build_request(request_template, opts) do
    RequestHelpers.build_v5_request(request_template, opts)
  end
end
