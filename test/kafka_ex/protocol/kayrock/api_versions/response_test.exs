defmodule KafkaEx.Protocol.Kayrock.ApiVersions.ResponseTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Protocol.Kayrock.ApiVersions
  alias KafkaEx.Messages.ApiVersions, as: ApiVersionsStruct
  alias KafkaEx.Client.Error

  describe "V0 response parsing - success cases" do
    test "parses successful V0 response with multiple APIs" do
      response = %Kayrock.ApiVersions.V0.Response{
        error_code: 0,
        api_versions: [
          %{api_key: 0, min_version: 0, max_version: 3},
          %{api_key: 1, min_version: 0, max_version: 11},
          %{api_key: 3, min_version: 0, max_version: 2},
          %{api_key: 18, min_version: 0, max_version: 1}
        ]
      }

      {:ok, result} = ApiVersions.Response.parse_response(response)

      assert %ApiVersionsStruct{} = result
      assert map_size(result.api_versions) == 4
      assert result.api_versions[0] == %{min_version: 0, max_version: 3}
      assert result.api_versions[1] == %{min_version: 0, max_version: 11}
      assert result.api_versions[3] == %{min_version: 0, max_version: 2}
      assert result.api_versions[18] == %{min_version: 0, max_version: 1}
      assert result.throttle_time_ms == nil
    end

    test "handles unsupported_version error" do
      response = %Kayrock.ApiVersions.V0.Response{
        error_code: 35,
        # :unsupported_version
        api_versions: []
      }

      {:error, error} = ApiVersions.Response.parse_response(response)

      assert %Error{} = error
      assert error.error == :unsupported_version
    end
  end

  describe "V1 response parsing - success cases" do
    test "parses successful V1 response with throttle_time_ms" do
      response = %Kayrock.ApiVersions.V1.Response{
        error_code: 0,
        api_versions: [
          %{api_key: 3, min_version: 0, max_version: 2}
        ],
        throttle_time_ms: 100
      }

      {:ok, result} = ApiVersions.Response.parse_response(response)

      assert %ApiVersionsStruct{} = result
      assert result.api_versions == %{3 => %{min_version: 0, max_version: 2}}
      assert result.throttle_time_ms == 100
    end

    test "handles V1 error with throttle_time_ms" do
      response = %Kayrock.ApiVersions.V1.Response{
        error_code: 35,
        api_versions: [],
        throttle_time_ms: 1000
      }

      {:error, error} = ApiVersions.Response.parse_response(response)

      assert %Error{} = error
      assert error.error == :unsupported_version
    end
  end
end
