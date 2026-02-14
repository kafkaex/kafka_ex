defimpl KafkaEx.Protocol.Kayrock.Produce.Response, for: Any do
  @moduledoc """
  Fallback implementation of Produce Response protocol for unknown future versions.

  All known Kayrock-supported versions (V0-V8) have explicit protocol
  implementations. This `Any` fallback exists solely for forward compatibility
  with potential future Kayrock versions that don't yet have an explicit impl.

  Extracts all standard response fields (base_offset, log_append_time,
  log_start_offset, throttle_time_ms) when available in the response struct.
  """

  alias KafkaEx.Protocol.Kayrock.Produce.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_response(response, fn resp, partition_resp ->
      fields = []

      fields =
        if Map.has_key?(partition_resp, :log_append_time) do
          [{:log_append_time, partition_resp.log_append_time} | fields]
        else
          fields
        end

      fields =
        if Map.has_key?(partition_resp, :log_start_offset) do
          [{:log_start_offset, partition_resp.log_start_offset} | fields]
        else
          fields
        end

      if Map.has_key?(resp, :throttle_time_ms) do
        [{:throttle_time_ms, resp.throttle_time_ms} | fields]
      else
        fields
      end
    end)
  end
end
