defimpl KafkaEx.Protocol.Kayrock.ListOffsets.Request, for: [Kayrock.ListOffsets.V2.Request] do
  @moduledoc """
  Implementation for ListOffsets V2 Request.

  V2 introduces `isolation_level` in the request:
  - `replica_id`, `isolation_level`
  - `topics` with partitions containing `partition` and `timestamp`
  """

  alias KafkaEx.Protocol.Kayrock.ListOffsets.RequestHelpers

  def build_request(request_template, opts) do
    RequestHelpers.build_request_v2_plus(request_template, opts, 2)
  end
end
