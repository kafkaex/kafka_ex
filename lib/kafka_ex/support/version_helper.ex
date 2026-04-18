defmodule KafkaEx.Support.VersionHelper do
  @moduledoc false

  @doc """
  Conditionally adds `:api_version` to `opts` based on the negotiated API versions map.

  If `key` is present in `api_versions`, puts `{:api_version, version}` into `opts`.
  Otherwise returns `opts` unchanged. Version `0` is treated as a valid version.
  """
  @spec maybe_put_api_version(Keyword.t(), map(), atom()) :: Keyword.t()
  def maybe_put_api_version(opts, api_versions, key) do
    case Map.get(api_versions, key) do
      nil -> opts
      version -> Keyword.put(opts, :api_version, version)
    end
  end
end
