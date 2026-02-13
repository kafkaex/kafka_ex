defimpl KafkaEx.Protocol.Kayrock.Fetch.Response, for: Any do
  @moduledoc """
  Fallback implementation of Fetch Response protocol for unknown future versions.

  All known Kayrock-supported versions (V0-V11) have explicit protocol
  implementations. This `Any` fallback exists solely for forward compatibility
  with potential future Kayrock versions that don't yet have an explicit impl.

  Extracts all available response fields dynamically by checking whether
  they exist in the response and partition header structures.
  """

  alias KafkaEx.Protocol.Kayrock.Fetch.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_response(response, fn resp, partition_resp ->
      partition_header = Map.get(partition_resp, :partition_header, %{})

      fields = []

      fields =
        if Map.has_key?(resp, :throttle_time_ms) do
          [{:throttle_time_ms, resp.throttle_time_ms} | fields]
        else
          fields
        end

      fields =
        if Map.has_key?(partition_header, :last_stable_offset) do
          [{:last_stable_offset, partition_header.last_stable_offset} | fields]
        else
          fields
        end

      fields =
        if Map.has_key?(partition_header, :log_start_offset) do
          [{:log_start_offset, partition_header.log_start_offset} | fields]
        else
          fields
        end

      fields =
        if Map.has_key?(partition_header, :aborted_transactions) do
          [{:aborted_transactions, partition_header.aborted_transactions} | fields]
        else
          fields
        end

      fields =
        if Map.has_key?(partition_header, :preferred_read_replica) do
          [{:preferred_read_replica, partition_header.preferred_read_replica} | fields]
        else
          fields
        end

      fields
    end)
  end
end
