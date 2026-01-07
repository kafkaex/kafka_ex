defmodule KafkaEx.Protocol.Kayrock.CreateTopics.RequestHelpers do
  @moduledoc """
  Shared helper functions for building CreateTopics requests across all versions.
  """

  @doc """
  Extracts topic configurations from options.

  ## Required Options
  - `:topics` - List of topic configurations, each being a map or keyword list with:
    - `:topic` - Topic name (required)
    - `:num_partitions` - Number of partitions (default: -1 for broker default)
    - `:replication_factor` - Replication factor (default: -1 for broker default)
    - `:replica_assignment` - Manual replica assignment (default: [])
    - `:config_entries` - Topic configuration entries (default: [])

  - `:timeout` - Request timeout in milliseconds (required)

  ## Optional
  - `:validate_only` - If true, only validate the request without creating topics (V1+)
  """
  @spec extract_common_fields(Keyword.t()) :: %{
          topics: list(),
          timeout: non_neg_integer()
        }
  def extract_common_fields(opts) do
    %{
      topics: Keyword.fetch!(opts, :topics),
      timeout: Keyword.fetch!(opts, :timeout)
    }
  end

  @doc """
  Converts topic configuration to Kayrock format.
  """
  @spec build_topic_request(map() | Keyword.t()) :: map()
  def build_topic_request(topic_config) when is_list(topic_config) do
    build_topic_request(Map.new(topic_config))
  end

  def build_topic_request(topic_config) when is_map(topic_config) do
    %{
      topic: Map.fetch!(topic_config, :topic),
      num_partitions: Map.get(topic_config, :num_partitions, -1),
      replication_factor: Map.get(topic_config, :replication_factor, -1),
      replica_assignment: build_replica_assignments(Map.get(topic_config, :replica_assignment, [])),
      config_entries: build_config_entries(Map.get(topic_config, :config_entries, []))
    }
  end

  @doc """
  Converts replica assignments to Kayrock format.
  """
  @spec build_replica_assignments(list()) :: list()
  def build_replica_assignments(assignments) when is_list(assignments) do
    Enum.map(assignments, fn assignment ->
      case assignment do
        %{partition: partition, replicas: replicas} ->
          %{partition: partition, replicas: replicas}

        {partition, replicas} when is_list(replicas) ->
          %{partition: partition, replicas: replicas}

        _ ->
          assignment
      end
    end)
  end

  @doc """
  Converts config entries to Kayrock format.
  """
  @spec build_config_entries(list()) :: list()
  def build_config_entries(entries) when is_list(entries) do
    Enum.map(entries, fn entry ->
      case entry do
        %{config_name: name, config_value: value} ->
          %{config_name: name, config_value: value}

        {name, value} ->
          %{config_name: to_string(name), config_value: value}

        _ ->
          entry
      end
    end)
  end

  @doc """
  Builds a CreateTopics request for V1 and V2 (which have identical request schemas).

  Both V1 and V2 support:
  - create_topic_requests: array of topic configs
  - timeout: int32
  - validate_only: boolean

  The difference between V1 and V2 is only in the response format (V2 adds throttle_time_ms).
  """
  @spec build_v1_v2_request(struct(), Keyword.t()) :: struct()
  def build_v1_v2_request(request_template, opts) do
    %{topics: topics, timeout: timeout} = extract_common_fields(opts)
    validate_only = Keyword.get(opts, :validate_only, false)

    create_topic_requests = Enum.map(topics, &build_topic_request/1)

    request_template
    |> Map.put(:create_topic_requests, create_topic_requests)
    |> Map.put(:timeout, timeout)
    |> Map.put(:validate_only, validate_only)
  end
end
