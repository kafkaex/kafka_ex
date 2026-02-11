defimpl KafkaEx.Protocol.Kayrock.ApiVersions.Response, for: Kayrock.ApiVersions.V0.Response do
  @moduledoc """
  Parses ApiVersions V0 responses.
  """
  alias KafkaEx.Messages.ApiVersions
  alias KafkaEx.Client.Error
  alias Kayrock.ErrorCode

  @doc """
  Parses an ApiVersions V0 response into a KafkaEx struct.
  Converts the Kayrock response format into our internal ApiVersions struct with a map-based api_versions.
  """
  @spec parse_response(Kayrock.ApiVersions.V0.Response.t()) :: {:ok, ApiVersions.t()} | {:error, Error.t()}
  def parse_response(%{error_code: 0, api_keys: versions}) do
    apis = Enum.into(versions, %{}, &{&1.api_key, %{min_version: &1.min_version, max_version: &1.max_version}})
    {:ok, ApiVersions.build(api_versions: apis, throttle_time_ms: nil)}
  end

  def parse_response(%{error_code: error_code}) when error_code != 0 do
    {:error, Error.build(ErrorCode.code_to_atom(error_code), %{})}
  end
end
