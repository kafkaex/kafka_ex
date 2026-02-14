defimpl KafkaEx.Protocol.Kayrock.ListOffsets.Request, for: Any do
  @moduledoc """
  Fallback implementation of ListOffsets Request protocol for unknown future versions.

  All known Kayrock-supported versions (V0-V5) have explicit protocol
  implementations. This `Any` fallback exists solely for forward compatibility
  with potential future Kayrock versions that don't yet have an explicit impl.

  Detects the version range by checking for distinguishing struct fields:
  - `isolation_level` present: V2+ path (uses `build_request_v2_plus/3`)
  - Otherwise: V0-V1 path (basic list offsets)

  For V4+ vs V2-V3 detection within the V2+ path, uses `Keyword.get(opts, :api_version)`
  when available; defaults to V2 (safe baseline) when not provided.

  For V0 vs V1 detection within the V0-V1 path, uses `Keyword.get(opts, :api_version)`
  when available; defaults to V1 (more common pre-isolation version) when not provided.
  """

  import KafkaEx.Protocol.Kayrock.ResponseHelpers, only: [parse_time: 1]

  alias KafkaEx.Protocol.Kayrock.ListOffsets.RequestHelpers

  def build_request(request, opts) do
    if Map.has_key?(request, :isolation_level) do
      # V2+ style: determine version from opts or default to V2 (safe baseline)
      api_version = Keyword.get(opts, :api_version, 2)
      RequestHelpers.build_request_v2_plus(request, opts, api_version)
    else
      # V0-V1 style: no isolation_level
      build_v0_v1_request(request, opts)
    end
  end

  defp build_v0_v1_request(request, opts) do
    replica_id = Keyword.get(opts, :replica_id, -1)
    offset_num = Keyword.get(opts, :offset_num, 1)

    # Detect V0 vs V1: use explicit api_version from opts if available,
    # otherwise default to V1 (the more common pre-isolation version).
    # V0 is the only version with max_num_offsets in partition data.
    api_version = Keyword.get(opts, :api_version, 1)
    is_v0 = api_version == 0

    topics =
      opts
      |> Keyword.fetch!(:topics)
      |> Enum.map(fn {topic, partitions} ->
        %{
          topic: topic,
          partitions:
            Enum.map(partitions, fn partition_data ->
              partition_map = %{
                partition: partition_data.partition_num,
                timestamp: parse_time(partition_data.timestamp)
              }

              if is_v0 do
                Map.put(partition_map, :max_num_offsets, offset_num)
              else
                partition_map
              end
            end)
        }
      end)

    request
    |> Map.put(:replica_id, replica_id)
    |> Map.put(:topics, topics)
  end
end
