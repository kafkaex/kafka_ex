defmodule KafkaEx.Protocol.Kayrock.ListOffsets.ResponseHelpers do
  @moduledoc """
  Shared helper functions for parsing ListOffsets responses across all versions.

  Version differences:
  - V0: Uses `offsets` array (returns first offset or 0)
  - V1: Uses single `offset` field
  - V2-V3: Uses single `offset` field with `timestamp`
  - V4-V5: Same as V2 but adds `leader_epoch` in partition responses
  """

  import KafkaEx.Protocol.Kayrock.ResponseHelpers,
    only: [build_response: 1, fail_fast_iterate_topics: 3, fail_fast_iterate_partitions: 3]

  alias KafkaEx.Messages.Offset

  @type error_tuple :: {atom(), String.t(), non_neg_integer()}

  @doc """
  Parses a ListOffsets response using the provided offset extractor function.

  The offset_extractor takes a partition response map and returns either:
  - `{:ok, data}` where data is a map with :partition, :offset, :error_code, and optionally :timestamp
  - `{:error, {error_code, topic, partition}}`
  """
  @spec parse_response(map(), (String.t(), map() -> {:ok, Offset.t()} | {:error, error_tuple()})) ::
          {:ok, list(Offset.t())} | {:error, any()}
  def parse_response(%{responses: responses}, offset_extractor) do
    responses
    |> fail_fast_iterate_topics(
      &parse_partition_responses(&1, &2, offset_extractor),
      topic_key: :topic,
      partitions_key: :partition_responses
    )
    |> build_response()
  end

  # ------------------------------------------------------------------------------
  # Version-specific offset extractors
  # ------------------------------------------------------------------------------

  @doc """
  Extracts offset from V0 response (offsets array).
  """
  @spec extract_v0_offset(String.t(), map()) :: {:ok, Offset.t()} | {:error, error_tuple()}
  def extract_v0_offset(topic, %{partition: partition, error_code: 0, offsets: []}) do
    data = %{partition: partition, offset: 0, error_code: :no_error}
    {:ok, Offset.from_list_offset(topic, [data])}
  end

  def extract_v0_offset(topic, %{partition: partition, error_code: 0, offsets: [offset | _]}) do
    data = %{partition: partition, offset: offset, error_code: :no_error}
    {:ok, Offset.from_list_offset(topic, [data])}
  end

  def extract_v0_offset(topic, %{error_code: error_code, partition: partition}) do
    {:error, {error_code, topic, partition}}
  end

  @doc """
  Extracts offset from V1 response (single offset field, no timestamp).
  """
  @spec extract_v1_offset(String.t(), map()) :: {:ok, Offset.t()} | {:error, error_tuple()}
  def extract_v1_offset(topic, %{error_code: 0, partition: partition, offset: offset}) do
    data = %{partition: partition, offset: offset, error_code: :no_error}
    {:ok, Offset.from_list_offset(topic, [data])}
  end

  def extract_v1_offset(topic, %{error_code: error_code, partition: partition}) do
    {:error, {error_code, topic, partition}}
  end

  @doc """
  Extracts offset from V2 response (single offset field with timestamp).
  """
  @spec extract_v2_offset(String.t(), map()) :: {:ok, Offset.t()} | {:error, error_tuple()}
  def extract_v2_offset(topic, %{error_code: 0, partition: partition, offset: offset, timestamp: timestamp}) do
    data = %{partition: partition, offset: offset, timestamp: timestamp, error_code: :no_error}
    {:ok, Offset.from_list_offset(topic, [data])}
  end

  def extract_v2_offset(topic, %{error_code: error_code, partition: partition}) do
    {:error, {error_code, topic, partition}}
  end

  @doc """
  Extracts offset from V4+ response (single offset field with timestamp and leader_epoch).

  V4 adds `leader_epoch` to the partition response, indicating the epoch of the leader
  at the time the offset was committed. A value of -1 means the leader epoch is unknown.

  This extractor is also used for V5 which has the same response schema.
  """
  @spec extract_v4_offset(String.t(), map()) :: {:ok, Offset.t()} | {:error, error_tuple()}
  def extract_v4_offset(
        topic,
        %{error_code: 0, partition: partition, offset: offset, timestamp: timestamp} = resp
      ) do
    data = %{
      partition: partition,
      offset: offset,
      timestamp: timestamp,
      error_code: :no_error,
      leader_epoch: Map.get(resp, :leader_epoch)
    }

    {:ok, Offset.from_list_offset(topic, [data])}
  end

  def extract_v4_offset(topic, %{error_code: error_code, partition: partition}) do
    {:error, {error_code, topic, partition}}
  end

  # ------------------------------------------------------------------------------
  # Private Functions
  # ------------------------------------------------------------------------------

  defp parse_partition_responses(topic, partition_responses, offset_extractor) do
    fail_fast_iterate_partitions(partition_responses, topic, offset_extractor)
  end
end
