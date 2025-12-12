defimpl KafkaEx.New.Protocols.Kayrock.Produce.Response, for: Kayrock.Produce.V0.Response do
  @moduledoc """
  Implementation for Produce V0 Response.

  V0 response only includes:
  - `base_offset` - The offset assigned to the first message
  - `error_code` - Error code (0 = success)

  No log_append_time, throttle_time_ms, or log_start_offset.
  """

  alias KafkaEx.New.Protocols.Kayrock.Produce.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_response(response, fn _resp, _partition_resp -> [] end)
  end
end
