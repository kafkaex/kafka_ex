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

  @doc """
  Adds current_leader_epoch to partition requests for V9+.

  V9 introduces `current_leader_epoch` in each partition request, allowing
  brokers to detect stale fetch requests from consumers that have not yet
  learned about a leader change.

  The default value of -1 means "no epoch specified" (unknown).
  """
  @spec add_current_leader_epoch(struct(), Keyword.t(), integer()) :: struct()
  def add_current_leader_epoch(request, opts, api_version) when api_version >= 9 do
    current_leader_epoch = Keyword.get(opts, :current_leader_epoch, -1)

    topics =
      Enum.map(request.topics, fn topic ->
        partitions =
          Enum.map(topic.partitions, fn partition ->
            Map.put(partition, :current_leader_epoch, current_leader_epoch)
          end)

        %{topic | partitions: partitions}
      end)

    %{request | topics: topics}
  end

  def add_current_leader_epoch(request, _opts, _api_version), do: request

  @doc """
  Adds rack_id for V11+ requests.

  V11 introduces `rack_id` at the top level, enabling rack-aware fetch
  so brokers can prefer replicas closer to the consumer.

  The default value is an empty string (no rack specified).
  """
  @spec add_rack_id(struct(), Keyword.t(), integer()) :: struct()
  def add_rack_id(request, opts, api_version) when api_version >= 11 do
    rack_id = Keyword.get(opts, :rack_id, "")
    %{request | rack_id: rack_id}
  end

  def add_rack_id(request, _opts, _api_version), do: request

  @doc """
  Builds a Fetch request for V7+ versions with all applicable fields.

  Handles the common pattern of V7+ requests which share:
  - V3+ max_bytes
  - V4+ isolation_level
  - V5+ log_start_offset in partition data
  - V7+ session fields (session_id, session_epoch, forgotten_topics_data)
  - V9+ current_leader_epoch in partition data
  - V11+ rack_id

  This helper reduces duplication across V7-V11 request implementations
  which all share the same base logic.
  """
  @spec build_request_v7_plus(struct(), Keyword.t(), integer()) :: struct()
  def build_request_v7_plus(request, opts, api_version) do
    fields = extract_common_fields(opts)
    topics = build_topics(fields, Keyword.put(opts, :api_version, api_version))

    request
    |> populate_request(fields, topics)
    |> add_max_bytes(fields, api_version)
    |> add_isolation_level(opts, api_version)
    |> add_session_fields(opts, api_version)
    |> add_current_leader_epoch(opts, api_version)
    |> add_rack_id(opts, api_version)
  end
end
