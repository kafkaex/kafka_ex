defmodule KafkaEx.Protocol.ApiVersions do
  alias KafkaEx.Protocol
  require Logger

  # the ApiVersions message can also, itself, have different api versions
  @default_this_api_version 0
  @default_throttle_time 0

  @moduledoc """
  Implementation of the Kafka ApiVersions request and response APIs

  See: https://kafka.apache.org/protocol.html#The_Messages_ApiVersions

  Reference implementation in Java:
  https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/requests/ApiVersionsRequest.java
  https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/requests/ApiVersionsResponse.java

  """

  defmodule Response do
    @moduledoc false
    defstruct error_code: nil, api_versions: nil, throttle_time_ms: nil

    @type t :: %Response{
            error_code: atom,
            api_versions: [ApiVersion],
            throttle_time_ms: integer
          }
  end

  defmodule ApiVersion do
    @moduledoc false
    defstruct api_key: nil, min_version: nil, max_version: nil

    @type t :: %ApiVersion{
            api_key: integer,
            min_version: integer,
            max_version: integer
          }
  end

  @spec create_request(integer, binary, integer) :: iodata
  def create_request(
        correlation_id,
        client_id,
        this_api_version \\ @default_this_api_version
      )

  def create_request(correlation_id, client_id, 1),
    do: create_request(correlation_id, client_id, 0)

  def create_request(correlation_id, client_id, 2),
    do: create_request(correlation_id, client_id, 0)

  def create_request(correlation_id, client_id, 0) do
    [
      Protocol.create_request(:api_versions, correlation_id, client_id)
    ]
  end

  @spec parse_response(binary, integer) :: Response.t()
  def parse_response(binary, this_api_version \\ @default_this_api_version)

  def parse_response(
        <<_correlation_id::32-signed, error_code::16-signed, api_versions_count::32-signed,
          rest::binary>>,
        this_api_version
      ) do
    %{
      parse_rest_of_response(api_versions_count, rest, this_api_version)
      | error_code: Protocol.error(error_code)
    }
  end

  def parse_response(nil, _this_api_version) do
    %Response{error_code: :no_response}
  end

  defp parse_rest_of_response(api_versions_count, data, this_api_version) do
    {api_versions, remaining_data} =
      Protocol.Common.read_array(
        api_versions_count,
        data,
        &parse_one_api_version/1
      )

    case {this_api_version, remaining_data} do
      {0, ""} ->
        %Response{
          api_versions: api_versions,
          throttle_time_ms: @default_throttle_time
        }

      {v, <<throttle_time_ms::32-signed>>} when v in [1, 2] ->
        %Response{
          api_versions: api_versions,
          throttle_time_ms: throttle_time_ms
        }
    end
  end

  defp parse_one_api_version(
         <<api_key::16-signed, min_version::16-signed, max_version::16-signed, rest::binary>>
       ) do
    {%ApiVersion{
       api_key: api_key,
       min_version: min_version,
       max_version: max_version
     }, rest}
  end
end
