defmodule KafkaEx.New.ApiVersions do
  @moduledoc false

  def from_response(%{api_versions: api_versions}) do
    Enum.into(
      api_versions,
      %{},
      fn %{
           api_key: api_key,
           min_version: min_version,
           max_version: max_version
         } ->
        {api_key, {min_version, max_version}}
      end
    )
  end

  def max_supported_version(api_versions, api, default \\ 0)
      when is_atom(api) do
    api_key = Kayrock.KafkaSchemaMetadata.api_key(api)

    case Map.get(api_versions, api_key) do
      {_, vsn} -> vsn
      nil -> default
    end
  end
end
