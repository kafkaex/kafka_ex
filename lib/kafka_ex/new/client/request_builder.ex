defmodule KafkaEx.New.Client.RequestBuilder do
  @moduledoc """
  This module is used to build request for KafkaEx.New.Client.
  It's main decision point which protocol to use for building request and what
  is required version.
  """
  @protocol Application.compile_env(
              :kafka_ex,
              :protocol,
              KafkaEx.New.Protocols.KayrockProtocol
            )

  @default_api_version %{
    describe_groups: 1,
    list_offsets: 1,
    offset_fetch: 1,
    offset_commit: 1
  }

  alias KafkaEx.New.Client.State

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
  Builds request for Offset Commit API
  """
  @spec offset_commit_request(Keyword.t(), State.t()) :: {:ok, term} | {:error, :api_version_no_supported}
  def offset_commit_request(request_opts, state) do
    case get_api_version(state, :offset_commit, request_opts) do
      {:ok, api_version} ->
        group_id = Keyword.fetch!(request_opts, :group_id)
        topics = Keyword.fetch!(request_opts, :topics)

        opts = [group_id: group_id, topics: topics]

        # Add optional parameters based on API version
        opts =
          if api_version >= 1 do
            Keyword.merge(opts,
              generation_id: Keyword.get(request_opts, :generation_id, -1),
              member_id: Keyword.get(request_opts, :member_id, "")
            )
          else
            opts
          end

        opts =
          if api_version >= 2 do
            Keyword.put(opts, :retention_time, Keyword.get(request_opts, :retention_time, -1))
          else
            opts
          end

        req = @protocol.build_request(:offset_commit, api_version, opts)
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
