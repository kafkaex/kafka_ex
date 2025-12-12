defmodule KafkaEx.New.Client.RequestBuilder do
  @moduledoc """
  This module is used to build request for KafkaEx.New.Client.
  It's main decision point which protocol to use for building request and what
  is required version.
  """
  @protocol Application.compile_env(:kafka_ex, :protocol, KafkaEx.New.Protocols.KayrockProtocol)

  @default_api_version %{
    api_versions: 0,
    describe_groups: 1,
    fetch: 3,
    heartbeat: 1,
    join_group: 1,
    leave_group: 1,
    list_offsets: 1,
    metadata: 1,
    offset_fetch: 1,
    offset_commit: 1,
    produce: 3,
    sync_group: 1
  }

  alias KafkaEx.New.Client.State

  @doc """
  Builds request for ApiVersions API
  """
  @spec api_versions_request(Keyword.t(), State.t()) :: {:ok, term} | {:error, :api_version_no_supported}
  def api_versions_request(request_opts, state) do
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
  @spec describe_groups_request(Keyword.t(), State.t()) :: {:ok, term} | {:error, :api_version_no_supported}
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
  @spec lists_offset_request(Keyword.t(), State.t()) :: {:ok, term} | {:error, :api_version_no_supported}
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
  @spec offset_fetch_request(Keyword.t(), State.t()) :: {:ok, term} | {:error, :api_version_no_supported}
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
  @spec heartbeat_request(Keyword.t(), State.t()) :: {:ok, term} | {:error, :api_version_no_supported}
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
  @spec leave_group_request(Keyword.t(), State.t()) :: {:ok, term} | {:error, :api_version_no_supported}
  def leave_group_request(request_opts, state) do
    case get_api_version(state, :leave_group, request_opts) do
      {:ok, api_version} ->
        group_id = Keyword.fetch!(request_opts, :group_id)
        member_id = Keyword.fetch!(request_opts, :member_id)
        opts = [group_id: group_id, member_id: member_id]

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
  @spec join_group_request(Keyword.t(), State.t()) :: {:ok, term} | {:error, :api_version_no_supported}
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
  @spec sync_group_request(Keyword.t(), State.t()) :: {:ok, term} | {:error, :api_version_no_supported}
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
          {:ok, term} | {:error, :api_version_no_supported}
  def metadata_request(request_opts, state) do
    case get_api_version(state, :metadata, request_opts) do
      {:ok, api_version} ->
        topics = Keyword.get(request_opts, :topics)
        req = @protocol.build_request(:metadata, api_version, topics: topics)
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
  @spec produce_request(Keyword.t(), State.t()) :: {:ok, term} | {:error, :api_version_no_supported}
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
  @spec fetch_request(Keyword.t(), State.t()) :: {:ok, term} | {:error, :api_version_no_supported}
  def fetch_request(request_opts, state) do
    case get_api_version(state, :fetch, request_opts) do
      {:ok, api_version} ->
        opts =
          request_opts
          |> Keyword.put(:api_version, api_version)

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
  @spec offset_commit_request(Keyword.t(), State.t()) :: {:ok, term} | {:error, :api_version_no_supported}
  def offset_commit_request(request_opts, state) do
    case get_api_version(state, :offset_commit, request_opts) do
      {:ok, api_version} ->
        req = @protocol.build_request(:offset_commit, api_version, request_opts)
        {:ok, req}

      {:error, error_code} ->
        {:error, error_code}
    end
  end

  # -----------------------------------------------------------------------------
  defp get_api_version(state, request_type, request_opts) do
    default = Map.fetch!(@default_api_version, request_type)
    requested_version = Keyword.get(request_opts, :api_version, default)
    max_supported = State.max_supported_api_version(state, request_type, default)

    if requested_version > max_supported do
      {:error, :api_version_no_supported}
    else
      {:ok, requested_version}
    end
  end
end
