defimpl KafkaEx.Protocol.Kayrock.Produce.Response, for: Kayrock.Produce.V1.Response do
  @moduledoc """
  Implementation for Produce V1 Response.

  V1 response adds:
  - `throttle_time_ms` - Time in ms the request was throttled due to quota violations

  Does not include log_append_time or log_start_offset.
  """

  alias KafkaEx.Protocol.Kayrock.Produce.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_response(response, fn resp, _partition_resp ->
      [throttle_time_ms: resp.throttle_time_ms]
    end)
  end
end
