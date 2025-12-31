defmodule KafkaEx.Messages.ApiVersions do
  @moduledoc """
  Represents the API versions supported by a Kafka broker.

  This struct contains information about which Kafka API versions
  the broker supports, enabling the client to negotiate compatible
  API versions for all operations.
  """

  defstruct [:api_versions, :throttle_time_ms]

  @typedoc """
  The Kafka API key identifier (e.g., 0 for Produce, 1 for Fetch, 3 for Metadata, 18 for ApiVersions)
  """
  @type api_key :: non_neg_integer()

  @typedoc """
  A single API version entry
    - `min_version`: Minimum supported version for this API
    - `max_version`: Maximum supported version for this API
  """
  @type api_version :: %{min_version: non_neg_integer(), max_version: non_neg_integer()}
  @type t :: %__MODULE__{api_versions: %{api_key => api_version}, throttle_time_ms: nil | non_neg_integer()}

  @doc """
  Builds an ApiVersions struct from response data.
  """
  @spec build(Keyword.t()) :: t()
  def build(opts \\ []) do
    %__MODULE__{
      api_versions: Keyword.get(opts, :api_versions, %{}),
      throttle_time_ms: Keyword.get(opts, :throttle_time_ms)
    }
  end

  @doc """
  Finds the maximum supported version for a given API.
  """
  @spec max_version_for_api(t(), non_neg_integer()) :: {:ok, non_neg_integer()} | {:error, :unsupported_api}
  def max_version_for_api(%__MODULE__{api_versions: api_versions}, api_key) do
    case Map.get(api_versions, api_key) do
      %{max_version: max_version} -> {:ok, max_version}
      nil -> {:error, :unsupported_api}
    end
  end

  @doc """
  Finds the minimum supported version for a given API.
  """
  @spec min_version_for_api(t(), non_neg_integer()) :: {:ok, non_neg_integer()} | {:error, :unsupported_api}
  def min_version_for_api(%__MODULE__{api_versions: api_versions}, api_key) do
    case Map.get(api_versions, api_key) do
      %{min_version: min_version} -> {:ok, min_version}
      nil -> {:error, :unsupported_api}
    end
  end

  @doc """
  Checks if a specific API version is supported.
  """
  @spec version_supported?(t(), non_neg_integer(), non_neg_integer()) :: boolean()
  def version_supported?(%__MODULE__{api_versions: api_versions}, api_key, version) do
    case Map.get(api_versions, api_key) do
      %{min_version: min_version, max_version: max_version} ->
        version >= min_version and version <= max_version

      nil ->
        false
    end
  end
end
