defimpl KafkaEx.Protocol.Kayrock.ApiVersions.Response, for: Kayrock.ApiVersions.V1.Response do
  @moduledoc """
  Parses ApiVersions V1 responses.
  """

  alias KafkaEx.Messages.ApiVersions
  alias KafkaEx.Client.Error
  alias Kayrock.ErrorCode

  @doc """
  Parses an ApiVersions V1 response into a KafkaEx struct.
  """
  @spec parse_response(Kayrock.ApiVersions.V1.Response.t()) :: {:ok, ApiVersions.t()} | {:error, Error.t()}
  def parse_response(%{error_code: 0, api_keys: versions, throttle_time_ms: throttle_time_ms}) do
    apis = Enum.into(versions, %{}, &{&1.api_key, %{min_version: &1.min_version, max_version: &1.max_version}})
    {:ok, ApiVersions.build(api_versions: apis, throttle_time_ms: throttle_time_ms)}
  end

  def parse_response(%{error_code: error_code}) when error_code != 0 do
    {:error, Error.build(ErrorCode.code_to_atom(error_code), %{})}
  end
end
