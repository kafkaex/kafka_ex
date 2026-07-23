defmodule KafkaEx.Client.MetadataLog do
  @moduledoc false
  # Pure decision helpers for metadata-refresh logging. No side effects.

  alias KafkaEx.API, as: KafkaExAPI
  alias KafkaEx.Cluster.ClusterMetadata

  # Log at most once per this window while the same set of topics stays missing.
  @heartbeat_ms 15 * 60 * 1000

  @spec heartbeat_ms() :: pos_integer()
  def heartbeat_ms, do: @heartbeat_ms

  @spec missing_topics([KafkaExAPI.topic_name()], ClusterMetadata.t()) :: [KafkaExAPI.topic_name()]
  def missing_topics(requested, %ClusterMetadata{topics: known}) do
    Enum.reject(requested, &Map.has_key?(known, &1))
  end

  @spec should_log?(MapSet.t(), MapSet.t(), integer() | nil, integer(), integer()) :: boolean()
  def should_log?(missing, prev_missing, prev_logged_at, now_ms, heartbeat_ms \\ @heartbeat_ms) do
    cond do
      not MapSet.equal?(missing, prev_missing) -> true
      is_nil(prev_logged_at) -> true
      now_ms - prev_logged_at >= heartbeat_ms -> true
      true -> false
    end
  end
end
