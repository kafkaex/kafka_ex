defmodule KafkaEx.Protocol.Kayrock.ListOffsets.RequestHelpers do
  @moduledoc """
  Shared utility functions for building ListOffsets requests.

  These are low-level utilities used by the version-specific request
  implementations for V2+ (which share the isolation_level field).

  ## Version-specific Kayrock schema differences

  - **V2-V3**: `replica_id, isolation_level, topics: [{topic, partitions: [{partition, timestamp}]}]`
  - **V4-V5**: Same as V3 but partitions include `current_leader_epoch`

  Note: Kayrock's V3 schema does not include `current_leader_epoch` in
  partitions (it was added in Kayrock's V4). The `build_request_v2_plus/3`
  helper conditionally adds this field based on the api_version parameter.
  """

  import KafkaEx.Protocol.Kayrock.ResponseHelpers, only: [parse_time: 1]

  @isolation_level %{read_uncommited: 0, read_commited: 1}

  @doc """
  Builds a ListOffsets request for V2+ versions with all applicable fields.

  Handles the common pattern of V2+ requests which share:
  - `replica_id` (all versions)
  - `isolation_level` (V2+)
  - `topics` with partitions containing `partition` and `timestamp`
  - `current_leader_epoch` in partitions (V4+ only, when Kayrock schema supports it)

  This helper reduces duplication across V2-V5 request implementations.
  """
  @spec build_request_v2_plus(struct(), Keyword.t(), integer()) :: struct()
  def build_request_v2_plus(request_template, opts, api_version) do
    replica_id = Keyword.get(opts, :replica_id, -1)
    isolation_level = Keyword.get(opts, :isolation_level)
    kafka_isolation_level = Map.get(@isolation_level, isolation_level, 0)

    topics = build_topics(opts, api_version)

    request_template
    |> Map.put(:replica_id, replica_id)
    |> Map.put(:isolation_level, kafka_isolation_level)
    |> Map.put(:topics, topics)
  end

  @doc """
  Builds the topics structure for ListOffsets requests.

  For V2-V3: partitions contain `partition` and `timestamp`.
  For V4+: partitions also contain `current_leader_epoch`.
  """
  @spec build_topics(Keyword.t(), integer()) :: list(map())
  def build_topics(opts, api_version) do
    current_leader_epoch = Keyword.get(opts, :current_leader_epoch, -1)

    opts
    |> Keyword.fetch!(:topics)
    |> Enum.map(fn {topic, partitions} ->
      %{
        topic: topic,
        partitions: Enum.map(partitions, &build_partition(&1, api_version, current_leader_epoch))
      }
    end)
  end

  defp build_partition(partition_data, api_version, current_leader_epoch) when api_version >= 4 do
    %{
      partition: partition_data.partition_num,
      current_leader_epoch: current_leader_epoch,
      timestamp: parse_time(partition_data.timestamp)
    }
  end

  defp build_partition(partition_data, _api_version, _current_leader_epoch) do
    %{
      partition: partition_data.partition_num,
      timestamp: parse_time(partition_data.timestamp)
    }
  end
end
