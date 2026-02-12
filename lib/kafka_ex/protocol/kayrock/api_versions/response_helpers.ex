defmodule KafkaEx.Protocol.Kayrock.ApiVersions.ResponseHelpers do
  @moduledoc """
  Shared helper functions for parsing ApiVersions responses across protocol versions.

  All ApiVersions response versions share the same core parsing logic:
  - Convert `api_keys` list to a map keyed by api_key
  - Extract `throttle_time_ms` (V1+; nil for V0)
  - Map error codes to tagged error tuples

  This module extracts that logic so individual version implementations
  can delegate here instead of duplicating code.
  """

  alias KafkaEx.Client.Error
  alias KafkaEx.Messages.ApiVersions
  alias Kayrock.ErrorCode

  @doc """
  Parses an ApiVersions response that includes `throttle_time_ms` (V1+).

  ## Returns

  - `{:ok, %ApiVersions{}}` on success (error_code == 0)
  - `{:error, %Error{}}` on Kafka error
  """
  @spec parse(map()) :: {:ok, ApiVersions.t()} | {:error, Error.t()}
  def parse(%{error_code: 0, api_keys: versions, throttle_time_ms: throttle_time_ms}) do
    apis = build_api_versions_map(versions)
    {:ok, ApiVersions.build(api_versions: apis, throttle_time_ms: throttle_time_ms)}
  end

  def parse(%{error_code: error_code}) when error_code != 0 do
    {:error, Error.build(ErrorCode.code_to_atom(error_code), %{})}
  end

  @doc """
  Parses an ApiVersions V0 response (no `throttle_time_ms` field).

  ## Returns

  - `{:ok, %ApiVersions{}}` on success (error_code == 0)
  - `{:error, %Error{}}` on Kafka error
  """
  @spec parse_v0(map()) :: {:ok, ApiVersions.t()} | {:error, Error.t()}
  def parse_v0(%{error_code: 0, api_keys: versions}) do
    apis = build_api_versions_map(versions)
    {:ok, ApiVersions.build(api_versions: apis, throttle_time_ms: nil)}
  end

  def parse_v0(%{error_code: error_code}) when error_code != 0 do
    {:error, Error.build(ErrorCode.code_to_atom(error_code), %{})}
  end

  @doc """
  Converts a list of api_key entries into a map keyed by api_key.
  """
  @spec build_api_versions_map([map()]) :: %{non_neg_integer() => ApiVersions.api_version()}
  def build_api_versions_map(versions) when is_list(versions) do
    Enum.into(versions, %{}, fn entry ->
      {entry.api_key, %{min_version: entry.min_version, max_version: entry.max_version}}
    end)
  end
end
