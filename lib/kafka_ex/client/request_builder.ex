defmodule KafkaEx.Client.RequestBuilder do
  @moduledoc """
  This module is used to build request for KafkaEx.Client.
  It's main decision point which protocol to use for building request and what
  is required version.
  """
  @protocol Application.compile_env(:kafka_ex, :protocol, KafkaEx.Protocol.KayrockProtocol)

  require Logger

  alias KafkaEx.Client.State

  @doc """
  Builds request for ApiVersions API.

  ApiVersions is special — it's how the client learns broker-supported
  versions of every OTHER API. Defaulting it to the broker-negotiated
  max is a bootstrapping hazard (chicken-and-egg before cache is
  populated) AND runs into KIP-511 V3 issues where brokers reject
  requests missing the required client software name/version tagged
  fields. We pin to V0 unless the caller explicitly opts into a
  higher version via `:api_version`.
  """
  @spec api_versions_request(Keyword.t(), State.t()) ::
          {:ok, term} | {:error, :api_version_no_supported | :api_not_supported_by_broker}
  def api_versions_request(request_opts, state) do
    request_opts = Keyword.put_new(request_opts, :api_version, 0)

    case get_api_version(state, :api_versions, request_opts) do
      {:ok, api_version} ->
        req = @protocol.build_request(:api_versions, api_version, [])
        {:ok, req}

      {:error, error_code} ->
        {:error, error_code}
    end
  end

  @doc """
  Builds request for Describe Groups API
  """
  @spec describe_groups_request(Keyword.t(), State.t()) ::
          {:ok, term} | {:error, :api_version_no_supported | :api_not_supported_by_broker}
  def describe_groups_request(request_opts, state) do
    case get_api_version(state, :describe_groups, request_opts) do
      {:ok, api_version} ->
        group_names = Keyword.fetch!(request_opts, :group_names)
        req = @protocol.build_request(:describe_groups, api_version, group_names: group_names)
        {:ok, req}

      {:error, error_code} ->
        {:error, error_code}
    end
  end

  @doc """
  Builds request for List Offsets API
  """
  @spec lists_offset_request(Keyword.t(), State.t()) ::
          {:ok, term} | {:error, :api_version_no_supported | :api_not_supported_by_broker}
  def lists_offset_request(request_opts, state) do
    case get_api_version(state, :list_offsets, request_opts) do
      {:ok, api_version} ->
        topics = Keyword.fetch!(request_opts, :topics)
        req = @protocol.build_request(:list_offsets, api_version, topics: topics)
        {:ok, req}

      {:error, error_code} ->
        {:error, error_code}
    end
  end

  @doc """
  Builds request for Offset Fetch API
  """
  @spec offset_fetch_request(Keyword.t(), State.t()) ::
          {:ok, term} | {:error, :api_version_no_supported | :api_not_supported_by_broker}
  def offset_fetch_request(request_opts, state) do
    case get_api_version(state, :offset_fetch, request_opts) do
      {:ok, api_version} ->
        group_id = Keyword.fetch!(request_opts, :group_id)
        topics = Keyword.fetch!(request_opts, :topics)
        req = @protocol.build_request(:offset_fetch, api_version, group_id: group_id, topics: topics)
        {:ok, req}

      {:error, error_code} ->
        {:error, error_code}
    end
  end

  @doc """
  Builds request for Heartbeat API
  """
  @spec heartbeat_request(Keyword.t(), State.t()) ::
          {:ok, term} | {:error, :api_version_no_supported | :api_not_supported_by_broker}
  def heartbeat_request(request_opts, state) do
    case get_api_version(state, :heartbeat, request_opts) do
      {:ok, api_version} ->
        group_id = Keyword.fetch!(request_opts, :group_id)
        member_id = Keyword.fetch!(request_opts, :member_id)
        generation_id = Keyword.fetch!(request_opts, :generation_id)

        opts = [group_id: group_id, member_id: member_id, generation_id: generation_id]

        req = @protocol.build_request(:heartbeat, api_version, opts)
        {:ok, req}

      {:error, error_code} ->
        {:error, error_code}
    end
  end

  @doc """
  Builds request for LeaveGroup API
  """
  @spec leave_group_request(Keyword.t(), State.t()) ::
          {:ok, term} | {:error, :api_version_no_supported | :api_not_supported_by_broker}
  def leave_group_request(request_opts, state) do
    case get_api_version(state, :leave_group, request_opts) do
      {:ok, api_version} ->
        group_id = Keyword.fetch!(request_opts, :group_id)
        member_id = Keyword.fetch!(request_opts, :member_id)

        opts =
          if api_version >= 3 do
            # KIP-345: V3+ uses members array instead of single member_id
            members = Keyword.get(request_opts, :members, [%{member_id: member_id}])
            [group_id: group_id, members: members]
          else
            [group_id: group_id, member_id: member_id]
          end

        req = @protocol.build_request(:leave_group, api_version, opts)
        {:ok, req}

      {:error, error_code} ->
        {:error, error_code}
    end
  end

  @doc """
  Builds request for JoinGroup API

  Version-specific logic is handled by the protocol layer (RequestHelpers).
  This function simply passes all options through to the protocol.
  """
  @spec join_group_request(Keyword.t(), State.t()) ::
          {:ok, term} | {:error, :api_version_no_supported | :api_not_supported_by_broker}
  def join_group_request(request_opts, state) do
    case get_api_version(state, :join_group, request_opts) do
      {:ok, api_version} ->
        req = @protocol.build_request(:join_group, api_version, request_opts)
        {:ok, req}

      {:error, error_code} ->
        {:error, error_code}
    end
  end

  @doc """
  Builds request for SyncGroup API
  """
  @spec sync_group_request(Keyword.t(), State.t()) ::
          {:ok, term} | {:error, :api_version_no_supported | :api_not_supported_by_broker}
  def sync_group_request(request_opts, state) do
    case get_api_version(state, :sync_group, request_opts) do
      {:ok, api_version} ->
        group_id = Keyword.fetch!(request_opts, :group_id)
        generation_id = Keyword.fetch!(request_opts, :generation_id)
        member_id = Keyword.fetch!(request_opts, :member_id)
        group_assignment = Keyword.get(request_opts, :group_assignment, [])

        opts = [
          group_id: group_id,
          generation_id: generation_id,
          member_id: member_id,
          group_assignment: group_assignment
        ]

        req = @protocol.build_request(:sync_group, api_version, opts)
        {:ok, req}

      {:error, error_code} ->
        {:error, error_code}
    end
  end

  @doc """
  Builds request for Metadata API
  """
  @spec metadata_request(Keyword.t(), State.t()) ::
          {:ok, term} | {:error, :api_version_no_supported | :api_not_supported_by_broker}
  def metadata_request(request_opts, state) do
    case get_api_version(state, :metadata, request_opts) do
      {:ok, api_version} ->
        topics = Keyword.get(request_opts, :topics)
        allow_auto_topic_creation = Keyword.get(request_opts, :allow_auto_topic_creation, false)

        opts = [topics: topics, allow_auto_topic_creation: allow_auto_topic_creation]
        req = @protocol.build_request(:metadata, api_version, opts)
        {:ok, req}

      {:error, error_code} ->
        {:error, error_code}
    end
  end

  @doc """
  Builds request for Produce API

  Version-specific logic is handled by the protocol layer (RequestHelpers).
  This function simply passes all options through to the protocol.
  """
  @spec produce_request(Keyword.t(), State.t()) ::
          {:ok, term} | {:error, :api_version_no_supported | :api_not_supported_by_broker}
  def produce_request(request_opts, state) do
    case get_api_version(state, :produce, request_opts) do
      {:ok, api_version} ->
        req = @protocol.build_request(:produce, api_version, request_opts)
        {:ok, req}

      {:error, error_code} ->
        {:error, error_code}
    end
  end

  @doc """
  Builds request for Fetch API

  Version-specific logic is handled by the protocol layer (RequestHelpers).
  This function simply passes all options through to the protocol.
  """
  @spec fetch_request(Keyword.t(), State.t()) ::
          {:ok, term} | {:error, :api_version_no_supported | :api_not_supported_by_broker}
  def fetch_request(request_opts, state) do
    case get_api_version(state, :fetch, request_opts) do
      {:ok, api_version} ->
        opts = request_opts |> Keyword.put(:api_version, api_version)
        req = @protocol.build_request(:fetch, api_version, opts)
        {:ok, req}

      {:error, error_code} ->
        {:error, error_code}
    end
  end

  @doc """
  Builds request for Offset Commit API

  Version-specific logic is handled by the protocol layer (RequestHelpers).
  This function simply passes all options through to the protocol.
  """
  @spec offset_commit_request(Keyword.t(), State.t()) ::
          {:ok, term} | {:error, :api_version_no_supported | :api_not_supported_by_broker}
  def offset_commit_request(request_opts, state) do
    case get_api_version(state, :offset_commit, request_opts) do
      {:ok, api_version} ->
        req = @protocol.build_request(:offset_commit, api_version, request_opts)
        {:ok, req}

      {:error, error_code} ->
        {:error, error_code}
    end
  end

  @doc """
  Builds request for FindCoordinator API

  ## Options

  - `group_id` (required for V0, optional for V1): The consumer group ID
  - `coordinator_key` (optional, V1): The key to look up (defaults to group_id)
  - `coordinator_type` (optional, V1): 0 for group (default), 1 for transaction
  - `api_version` (optional): API version to use (default: negotiated max)
  """
  @spec find_coordinator_request(Keyword.t(), State.t()) ::
          {:ok, term} | {:error, :api_version_no_supported | :api_not_supported_by_broker}
  def find_coordinator_request(request_opts, state) do
    case get_api_version(state, :find_coordinator, request_opts) do
      {:ok, api_version} ->
        req = @protocol.build_request(:find_coordinator, api_version, request_opts)
        {:ok, req}

      {:error, error_code} ->
        {:error, error_code}
    end
  end

  @doc """
  Builds request for CreateTopics API

  ## Options

  - `topics` (required): List of topic configurations, each with:
    - `:topic` - Topic name (required)
    - `:num_partitions` - Number of partitions (default: -1 for broker default)
    - `:replication_factor` - Replication factor (default: -1 for broker default)
    - `:replica_assignment` - Manual replica assignment (default: [])
    - `:config_entries` - Topic configuration entries (default: [])
  - `timeout` (required): Request timeout in milliseconds
  - `validate_only` (optional, V1+): If true, only validate without creating
  - `api_version` (optional): API version to use (default: negotiated max)
  """
  @spec create_topics_request(Keyword.t(), State.t()) ::
          {:ok, term} | {:error, :api_version_no_supported | :api_not_supported_by_broker}
  def create_topics_request(request_opts, state) do
    case get_api_version(state, :create_topics, request_opts) do
      {:ok, api_version} ->
        req = @protocol.build_request(:create_topics, api_version, request_opts)
        {:ok, req}

      {:error, error_code} ->
        {:error, error_code}
    end
  end

  @doc """
  Builds request for Delete Topics API
  """
  @spec delete_topics_request(Keyword.t(), State.t()) ::
          {:ok, term} | {:error, :api_version_no_supported | :api_not_supported_by_broker}
  def delete_topics_request(request_opts, state) do
    case get_api_version(state, :delete_topics, request_opts) do
      {:ok, api_version} ->
        req = @protocol.build_request(:delete_topics, api_version, request_opts)
        {:ok, req}

      {:error, error_code} ->
        {:error, error_code}
    end
  end

  # -----------------------------------------------------------------------------
  # Version resolution chain:
  # 1. request_opts[:api_version] — explicit per-call override (tier 1)
  # 2. app config :api_versions map — deployment-wide override (tier 2)
  # 3. negotiated max = min(broker_max, kayrock_max) (tier 3)
  # Returns {:error, :api_not_supported_by_broker} when broker doesn't report the API.
  # Returns {:error, :api_version_no_supported} when requested version exceeds max.
  defp get_api_version(state, request_type, request_opts) do
    case State.max_supported_api_version(state, request_type) do
      {:error, :api_not_supported_by_broker} ->
        # If an explicit api_version was provided in request opts, honor it.
        # This is needed for bootstrap (ApiVersions request before state is populated).
        case Keyword.fetch(request_opts, :api_version) do
          {:ok, version} ->
            {:ok, version}

          :error ->
            Logger.error("Kafka API #{request_type} is not supported by the connected broker.")
            {:error, :api_not_supported_by_broker}
        end

      {:ok, max_supported} ->
        app_config = Application.get_env(:kafka_ex, :api_versions, %{})
        app_version = Map.get(app_config, request_type)

        effective_default =
          case app_version do
            nil -> max_supported
            version -> version
          end

        requested_version = Keyword.get(request_opts, :api_version, effective_default)

        if requested_version > max_supported do
          Logger.error(
            "Requested API version #{requested_version} for #{request_type} " <>
              "exceeds max supported version #{max_supported}."
          )

          {:error, :api_version_no_supported}
        else
          {:ok, requested_version}
        end
    end
  end
end
