defmodule KafkaEx.Protocol.Kayrock.Fetch.RequestHelpers do
  @moduledoc """
  Shared utility functions for building Fetch requests.

  These are low-level utilities used by the version-specific request
  implementations.
  """

  @doc """
  Extracts common fields from request options.

  Returns a map with:
  - `:topic` - The topic to fetch from
  - `:partition` - The partition to fetch from
  - `:offset` - The offset to start fetching from
  - `:max_bytes` - Maximum bytes to fetch per partition
  - `:max_wait_time` - Maximum time to wait for messages in ms
  - `:min_bytes` - Minimum bytes to accumulate before returning
  """
  @spec extract_common_fields(Keyword.t()) :: map()
  def extract_common_fields(opts) do
    %{
      topic: Keyword.fetch!(opts, :topic),
      partition: Keyword.fetch!(opts, :partition),
      offset: Keyword.fetch!(opts, :offset),
      max_bytes: Keyword.get(opts, :max_bytes, 1_000_000),
      max_wait_time: Keyword.get(opts, :max_wait_time, 10_000),
      min_bytes: Keyword.get(opts, :min_bytes, 1)
    }
  end

  @doc """
  Builds the topics structure for Fetch request.

  For V0-V4:
  ```
  [%{topic: "topic", partitions: [%{partition: 0, fetch_offset: 0, max_bytes: 1000000}]}]
  ```

  For V5+:
  ```
  [%{topic: "topic", partitions: [%{partition: 0, fetch_offset: 0, log_start_offset: 0, max_bytes: 1000000}]}]
  ```
  """
  @spec build_topics(map(), Keyword.t()) :: list(map())
  def build_topics(fields, opts) do
    api_version = Keyword.get(opts, :api_version, 0)

    partition_request =
      %{
        partition: fields.partition,
        fetch_offset: fields.offset,
        partition_max_bytes: fields.max_bytes
      }

    partition_request =
      if api_version >= 5 do
        log_start_offset = Keyword.get(opts, :log_start_offset, 0)
        Map.put(partition_request, :log_start_offset, log_start_offset)
      else
        partition_request
      end

    [
      %{
        topic: fields.topic,
        partitions: [partition_request]
      }
    ]
  end

  @doc """
  Populates a request struct with common fields.
  """
  @spec populate_request(struct(), map(), list(map())) :: struct()
  def populate_request(request, fields, topics) do
    %{
      request
      | replica_id: -1,
        max_wait_time: fields.max_wait_time,
        min_bytes: fields.min_bytes,
        topics: topics
    }
  end

  @doc """
  Adds max_bytes field for V3+ requests.
  """
  @spec add_max_bytes(struct(), map(), integer()) :: struct()
  def add_max_bytes(request, fields, api_version) when api_version >= 3 do
    # For V3+, max_bytes is at the request level as well
    request_max_bytes = Keyword.get([max_bytes: fields.max_bytes], :max_bytes, 10_485_760)
    %{request | max_bytes: request_max_bytes}
  end

  def add_max_bytes(request, _fields, _api_version), do: request

  @doc """
  Adds isolation_level field for V4+ requests.
  """
  @spec add_isolation_level(struct(), Keyword.t(), integer()) :: struct()
  def add_isolation_level(request, opts, api_version) when api_version >= 4 do
    # 0 = READ_UNCOMMITTED, 1 = READ_COMMITTED
    isolation_level = Keyword.get(opts, :isolation_level, 0)
    %{request | isolation_level: isolation_level}
  end

  def add_isolation_level(request, _opts, _api_version), do: request

  @doc """
  Adds session fields for V7+ requests (incremental fetch).
  """
  @spec add_session_fields(struct(), Keyword.t(), integer()) :: struct()
  def add_session_fields(request, opts, api_version) when api_version >= 7 do
    session_id = Keyword.get(opts, :session_id, 0)
    epoch = Keyword.get(opts, :epoch, -1)
    forgotten_topics_data = Keyword.get(opts, :forgotten_topics_data, [])

    %{
      request
      | session_id: session_id,
        session_epoch: epoch,
        forgotten_topics_data: forgotten_topics_data
    }
  end

  def add_session_fields(request, _opts, _api_version), do: request
end
