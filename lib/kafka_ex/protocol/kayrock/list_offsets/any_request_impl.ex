defimpl KafkaEx.Protocol.Kayrock.ListOffsets.Request, for: Any do
  @moduledoc """
  Fallback implementation of ListOffsets Request protocol for unknown future versions.

  All known Kayrock-supported versions (V0-V5) have explicit protocol
  implementations. This `Any` fallback exists solely for forward compatibility
  with potential future Kayrock versions that don't yet have an explicit impl.

  Detects the version range by checking for distinguishing struct fields:
  - `isolation_level` present with partition `current_leader_epoch`: V4+ path
  - `isolation_level` present without `current_leader_epoch`: V2-V3 path
  - Otherwise: V0-V1 path (basic list offsets)
  """

  import KafkaEx.Protocol.Kayrock.ResponseHelpers, only: [parse_time: 1]

  alias KafkaEx.Protocol.Kayrock.ListOffsets.RequestHelpers

  def build_request(request, opts) do
    if Map.has_key?(request, :isolation_level) do
      # V2+ style: determine version based on partition schema
      # Kayrock V4+ structs have current_leader_epoch in partition schema,
      # but we detect this from the api_version hint in opts
      api_version = detect_api_version(request, opts)
      RequestHelpers.build_request_v2_plus(request, opts, api_version)
    else
      # V0-V1 style: no isolation_level
      build_v0_v1_request(request, opts)
    end
  end

  # V4/V5 Kayrock structs include current_leader_epoch in partition data.
  # We detect this by checking if the Kayrock schema includes the field.
  # Since the struct topics field is initially empty, we check the struct module's schema.
  defp detect_api_version(request, opts) do
    # If caller provides explicit api_version, use that
    case Keyword.get(opts, :api_version) do
      nil ->
        # Heuristic: try to detect from the struct's module schema
        # V4+ modules have current_leader_epoch in partition schema
        if has_current_leader_epoch?(request), do: 4, else: 2

      version ->
        version
    end
  end

  defp has_current_leader_epoch?(request) do
    # Check the schema method if available on the struct's module
    struct_module = request.__struct__

    if function_exported?(struct_module, :schema, 0) do
      schema = struct_module.schema()
      topics_schema = Keyword.get(schema, :topics, nil)
      schema_has_leader_epoch?(topics_schema)
    else
      false
    end
  end

  defp schema_has_leader_epoch?({:array, topic_fields}) when is_list(topic_fields) do
    case Keyword.get(topic_fields, :partitions) do
      {:array, partition_fields} when is_list(partition_fields) ->
        Keyword.has_key?(partition_fields, :current_leader_epoch)

      _ ->
        false
    end
  end

  defp schema_has_leader_epoch?(_), do: false

  defp build_v0_v1_request(request, opts) do
    replica_id = Keyword.get(opts, :replica_id, -1)
    offset_num = Keyword.get(opts, :offset_num, 1)

    # Detect V0 vs V1 by checking for max_num_offsets in partition schema
    is_v0 =
      if function_exported?(request.__struct__, :schema, 0) do
        schema = request.__struct__.schema()
        topics_schema = Keyword.get(schema, :topics, nil)
        schema_has_max_num_offsets?(topics_schema)
      else
        false
      end

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

  defp schema_has_max_num_offsets?({:array, topic_fields}) when is_list(topic_fields) do
    case Keyword.get(topic_fields, :partitions) do
      {:array, partition_fields} when is_list(partition_fields) ->
        Keyword.has_key?(partition_fields, :max_num_offsets)

      _ ->
        false
    end
  end

  defp schema_has_max_num_offsets?(_), do: false
end
