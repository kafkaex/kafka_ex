defmodule KafkaEx.ApiVersions do
  @moduledoc false

  def api_versions_map(api_versions) do
    api_versions
    |> Enum.reduce(%{}, fn version, version_map ->
      version_map |> Map.put(version.api_key, version)
    end)
  end

  def find_api_version([:unsupported], _, {min_implemented_version, _}),
    do: {:ok, min_implemented_version}

  def find_api_version(
        api_versions_map,
        message_type,
        {min_implemented_version, max_implemented_version}
      ) do
    case KafkaEx.Protocol.api_key(message_type) do
      nil ->
        :unknown_message_for_client

      api_key ->
        case api_versions_map[api_key] do
          %{min_version: min} when min > max_implemented_version ->
            :no_version_supported

          %{max_version: max} when max < min_implemented_version ->
            :no_version_supported

          %{max_version: max} ->
            {:ok, Enum.min([max_implemented_version, max])}

          _ ->
            :unknown_message_for_server
        end
    end
  end
end
