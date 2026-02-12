defmodule KafkaEx.Protocol.Kayrock.ApiVersions.ResponseTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Protocol.Kayrock.ApiVersions
  alias KafkaEx.Messages.ApiVersions, as: ApiVersionsStruct
  alias KafkaEx.Client.Error

  describe "V0 response parsing - success cases" do
    test "parses successful V0 response with multiple APIs" do
      response = %Kayrock.ApiVersions.V0.Response{
        error_code: 0,
        api_keys: [
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

    test "parses V0 response with empty api_keys" do
      response = %Kayrock.ApiVersions.V0.Response{
        error_code: 0,
        api_keys: []
      }

      {:ok, result} = ApiVersions.Response.parse_response(response)

      assert result.api_versions == %{}
      assert result.throttle_time_ms == nil
    end

    test "handles unsupported_version error" do
      response = %Kayrock.ApiVersions.V0.Response{
        error_code: 35,
        # :unsupported_version
        api_keys: []
      }

      {:error, error} = ApiVersions.Response.parse_response(response)

      assert %Error{} = error
      assert error.error == :unsupported_version
    end

    test "handles generic error code" do
      response = %Kayrock.ApiVersions.V0.Response{
        error_code: -1,
        api_keys: []
      }

      {:error, error} = ApiVersions.Response.parse_response(response)

      assert %Error{} = error
      assert error.error == :unknown_server_error
    end
  end

  describe "V1 response parsing - success cases" do
    test "parses successful V1 response with throttle_time_ms" do
      response = %Kayrock.ApiVersions.V1.Response{
        error_code: 0,
        api_keys: [
          %{api_key: 3, min_version: 0, max_version: 2}
        ],
        throttle_time_ms: 100
      }

      {:ok, result} = ApiVersions.Response.parse_response(response)

      assert %ApiVersionsStruct{} = result
      assert result.api_versions == %{3 => %{min_version: 0, max_version: 2}}
      assert result.throttle_time_ms == 100
    end

    test "parses V1 response with empty api_keys" do
      response = %Kayrock.ApiVersions.V1.Response{
        error_code: 0,
        api_keys: [],
        throttle_time_ms: 0
      }

      {:ok, result} = ApiVersions.Response.parse_response(response)

      assert result.api_versions == %{}
      assert result.throttle_time_ms == 0
    end

    test "handles V1 error with throttle_time_ms" do
      response = %Kayrock.ApiVersions.V1.Response{
        error_code: 35,
        api_keys: [],
        throttle_time_ms: 1000
      }

      {:error, error} = ApiVersions.Response.parse_response(response)

      assert %Error{} = error
      assert error.error == :unsupported_version
    end

    test "handles generic error code" do
      response = %Kayrock.ApiVersions.V1.Response{
        error_code: -1,
        api_keys: [],
        throttle_time_ms: 0
      }

      {:error, error} = ApiVersions.Response.parse_response(response)

      assert %Error{} = error
      assert error.error == :unknown_server_error
    end
  end

  describe "V2 response parsing - success cases" do
    test "parses successful V2 response with multiple APIs" do
      response = %Kayrock.ApiVersions.V2.Response{
        error_code: 0,
        api_keys: [
          %{api_key: 0, min_version: 0, max_version: 8},
          %{api_key: 1, min_version: 0, max_version: 11},
          %{api_key: 3, min_version: 0, max_version: 9},
          %{api_key: 18, min_version: 0, max_version: 3}
        ],
        throttle_time_ms: 0
      }

      {:ok, result} = ApiVersions.Response.parse_response(response)

      assert %ApiVersionsStruct{} = result
      assert map_size(result.api_versions) == 4
      assert result.api_versions[0] == %{min_version: 0, max_version: 8}
      assert result.api_versions[1] == %{min_version: 0, max_version: 11}
      assert result.api_versions[3] == %{min_version: 0, max_version: 9}
      assert result.api_versions[18] == %{min_version: 0, max_version: 3}
      assert result.throttle_time_ms == 0
    end

    test "parses V2 response with zero throttle time" do
      response = %Kayrock.ApiVersions.V2.Response{
        error_code: 0,
        api_keys: [
          %{api_key: 3, min_version: 0, max_version: 2}
        ],
        throttle_time_ms: 0
      }

      {:ok, result} = ApiVersions.Response.parse_response(response)

      assert result.throttle_time_ms == 0
    end

    test "parses V2 response with empty api_keys" do
      response = %Kayrock.ApiVersions.V2.Response{
        error_code: 0,
        api_keys: [],
        throttle_time_ms: 0
      }

      {:ok, result} = ApiVersions.Response.parse_response(response)

      assert result.api_versions == %{}
      assert result.throttle_time_ms == 0
    end
  end

  describe "V2 response parsing - error cases" do
    test "handles unsupported_version error" do
      response = %Kayrock.ApiVersions.V2.Response{
        error_code: 35,
        api_keys: [],
        throttle_time_ms: 0
      }

      {:error, error} = ApiVersions.Response.parse_response(response)

      assert %Error{} = error
      assert error.error == :unsupported_version
    end

    test "handles generic error code" do
      response = %Kayrock.ApiVersions.V2.Response{
        error_code: -1,
        api_keys: [],
        throttle_time_ms: 0
      }

      {:error, error} = ApiVersions.Response.parse_response(response)

      assert %Error{} = error
      assert error.error == :unknown_server_error
    end
  end

  describe "V3 response parsing - success cases" do
    test "parses successful V3 response with compact array format" do
      response = %Kayrock.ApiVersions.V3.Response{
        error_code: 0,
        api_keys: [
          %{api_key: 0, min_version: 0, max_version: 8, tagged_fields: []},
          %{api_key: 1, min_version: 0, max_version: 11, tagged_fields: []},
          %{api_key: 3, min_version: 0, max_version: 9, tagged_fields: []},
          %{api_key: 18, min_version: 0, max_version: 3, tagged_fields: []}
        ],
        throttle_time_ms: 0,
        tagged_fields: []
      }

      {:ok, result} = ApiVersions.Response.parse_response(response)

      assert %ApiVersionsStruct{} = result
      assert map_size(result.api_versions) == 4
      assert result.api_versions[0] == %{min_version: 0, max_version: 8}
      assert result.api_versions[1] == %{min_version: 0, max_version: 11}
      assert result.api_versions[3] == %{min_version: 0, max_version: 9}
      assert result.api_versions[18] == %{min_version: 0, max_version: 3}
      assert result.throttle_time_ms == 0
    end

    test "parses V3 response with non-zero throttle time" do
      response = %Kayrock.ApiVersions.V3.Response{
        error_code: 0,
        api_keys: [
          %{api_key: 3, min_version: 0, max_version: 9, tagged_fields: []}
        ],
        throttle_time_ms: 250,
        tagged_fields: []
      }

      {:ok, result} = ApiVersions.Response.parse_response(response)

      assert result.throttle_time_ms == 250
      assert result.api_versions == %{3 => %{min_version: 0, max_version: 9}}
    end

    test "parses V3 response with empty api_keys" do
      response = %Kayrock.ApiVersions.V3.Response{
        error_code: 0,
        api_keys: [],
        throttle_time_ms: 0,
        tagged_fields: []
      }

      {:ok, result} = ApiVersions.Response.parse_response(response)

      assert result.api_versions == %{}
    end

    test "ignores unknown tagged_fields in response" do
      response = %Kayrock.ApiVersions.V3.Response{
        error_code: 0,
        api_keys: [
          %{api_key: 0, min_version: 0, max_version: 8, tagged_fields: [{42, <<1, 2, 3>>}]}
        ],
        throttle_time_ms: 0,
        tagged_fields: [{99, <<4, 5, 6>>}]
      }

      {:ok, result} = ApiVersions.Response.parse_response(response)

      assert %ApiVersionsStruct{} = result
      assert result.api_versions[0] == %{min_version: 0, max_version: 8}
    end

    test "parses V3 response with single API entry" do
      response = %Kayrock.ApiVersions.V3.Response{
        error_code: 0,
        api_keys: [
          %{api_key: 18, min_version: 0, max_version: 3, tagged_fields: []}
        ],
        throttle_time_ms: 0,
        tagged_fields: []
      }

      {:ok, result} = ApiVersions.Response.parse_response(response)

      assert map_size(result.api_versions) == 1
      assert result.api_versions[18] == %{min_version: 0, max_version: 3}
    end
  end

  describe "V3 response parsing - error cases" do
    test "handles unsupported_version error" do
      response = %Kayrock.ApiVersions.V3.Response{
        error_code: 35,
        api_keys: [],
        throttle_time_ms: 0,
        tagged_fields: []
      }

      {:error, error} = ApiVersions.Response.parse_response(response)

      assert %Error{} = error
      assert error.error == :unsupported_version
    end

    test "handles generic error code" do
      response = %Kayrock.ApiVersions.V3.Response{
        error_code: -1,
        api_keys: [],
        throttle_time_ms: 0,
        tagged_fields: []
      }

      {:error, error} = ApiVersions.Response.parse_response(response)

      assert %Error{} = error
      assert error.error == :unknown_server_error
    end
  end

  describe "Any fallback response implementation (forward compatibility)" do
    # The @fallback_to_any true on the Response protocol means any struct type
    # without an explicit implementation gets the Any fallback, which delegates
    # to ResponseHelpers.parse/1. This simulates a hypothetical future V4+ response.

    defmodule FakeV4Response do
      defstruct [:error_code, :api_keys, :throttle_time_ms, :new_field]
    end

    test "parses unknown struct via Any fallback using ResponseHelpers.parse/1" do
      response = %FakeV4Response{
        error_code: 0,
        api_keys: [
          %{api_key: 0, min_version: 0, max_version: 10},
          %{api_key: 18, min_version: 0, max_version: 4}
        ],
        throttle_time_ms: 75,
        new_field: "future_data"
      }

      {:ok, result} = ApiVersions.Response.parse_response(response)

      assert %ApiVersionsStruct{} = result

      assert result.api_versions == %{
               0 => %{min_version: 0, max_version: 10},
               18 => %{min_version: 0, max_version: 4}
             }

      assert result.throttle_time_ms == 75
    end

    test "Any fallback handles error code from unknown struct" do
      response = %FakeV4Response{
        error_code: 35,
        api_keys: [],
        throttle_time_ms: 0
      }

      {:error, error} = ApiVersions.Response.parse_response(response)

      assert %Error{} = error
      assert error.error == :unsupported_version
    end

    test "Any fallback works with a plain map" do
      # Maps also implement the Any protocol
      response = %{
        error_code: 0,
        api_keys: [%{api_key: 3, min_version: 0, max_version: 9}],
        throttle_time_ms: 0
      }

      {:ok, result} = ApiVersions.Response.parse_response(response)

      assert %ApiVersionsStruct{} = result
      assert result.api_versions == %{3 => %{min_version: 0, max_version: 9}}
    end

    test "Any fallback with empty api_keys from unknown struct" do
      response = %FakeV4Response{
        error_code: 0,
        api_keys: [],
        throttle_time_ms: 500
      }

      {:ok, result} = ApiVersions.Response.parse_response(response)

      assert result.api_versions == %{}
      assert result.throttle_time_ms == 500
    end
  end

  describe "additional error code coverage" do
    # Test various error codes to ensure the error path handles them properly

    test "V1 response with broker_not_available error (code 8)" do
      response = %Kayrock.ApiVersions.V1.Response{
        error_code: 8,
        api_keys: [],
        throttle_time_ms: 0
      }

      {:error, error} = ApiVersions.Response.parse_response(response)

      assert %Error{} = error
      assert is_atom(error.error)
      assert error.error != nil
    end

    test "V2 response with request_timed_out error (code 7)" do
      response = %Kayrock.ApiVersions.V2.Response{
        error_code: 7,
        api_keys: [],
        throttle_time_ms: 0
      }

      {:error, error} = ApiVersions.Response.parse_response(response)

      assert %Error{} = error
      assert is_atom(error.error)
    end

    test "V0 error response ignores api_keys content" do
      # When error_code != 0, the guard `when error_code != 0` matches
      # and the api_keys are not examined
      response = %Kayrock.ApiVersions.V0.Response{
        error_code: 35,
        api_keys: [
          %{api_key: 0, min_version: 0, max_version: 8}
        ]
      }

      {:error, error} = ApiVersions.Response.parse_response(response)

      assert %Error{} = error
      assert error.error == :unsupported_version
    end

    test "V1 error response ignores api_keys content" do
      response = %Kayrock.ApiVersions.V1.Response{
        error_code: -1,
        api_keys: [
          %{api_key: 0, min_version: 0, max_version: 8}
        ],
        throttle_time_ms: 5000
      }

      {:error, error} = ApiVersions.Response.parse_response(response)

      assert %Error{} = error
      assert error.error == :unknown_server_error
    end
  end

  describe "V1 response parsing - additional edge cases" do
    test "handles large throttle_time_ms" do
      response = %Kayrock.ApiVersions.V1.Response{
        error_code: 0,
        api_keys: [%{api_key: 0, min_version: 0, max_version: 1}],
        throttle_time_ms: 2_147_483_647
      }

      {:ok, result} = ApiVersions.Response.parse_response(response)

      assert result.throttle_time_ms == 2_147_483_647
    end

    test "handles many API key entries" do
      api_keys =
        for key <- 0..50 do
          %{api_key: key, min_version: 0, max_version: key + 1}
        end

      response = %Kayrock.ApiVersions.V1.Response{
        error_code: 0,
        api_keys: api_keys,
        throttle_time_ms: 0
      }

      {:ok, result} = ApiVersions.Response.parse_response(response)

      assert map_size(result.api_versions) == 51
      assert result.api_versions[0] == %{min_version: 0, max_version: 1}
      assert result.api_versions[50] == %{min_version: 0, max_version: 51}
    end
  end

  describe "parse_response via KayrockProtocol" do
    alias KafkaEx.Protocol.KayrockProtocol

    test "dispatches V0 response through parse_response" do
      response = %Kayrock.ApiVersions.V0.Response{
        error_code: 0,
        api_keys: [
          %{api_key: 0, min_version: 0, max_version: 3}
        ]
      }

      {:ok, result} = KayrockProtocol.parse_response(:api_versions, response)

      assert %ApiVersionsStruct{} = result
      assert result.api_versions[0] == %{min_version: 0, max_version: 3}
      assert result.throttle_time_ms == nil
    end

    test "dispatches V1 response through parse_response" do
      response = %Kayrock.ApiVersions.V1.Response{
        error_code: 0,
        api_keys: [
          %{api_key: 1, min_version: 0, max_version: 11}
        ],
        throttle_time_ms: 200
      }

      {:ok, result} = KayrockProtocol.parse_response(:api_versions, response)

      assert %ApiVersionsStruct{} = result
      assert result.api_versions[1] == %{min_version: 0, max_version: 11}
      assert result.throttle_time_ms == 200
    end

    test "dispatches V2 response through parse_response" do
      response = %Kayrock.ApiVersions.V2.Response{
        error_code: 0,
        api_keys: [
          %{api_key: 0, min_version: 0, max_version: 8}
        ],
        throttle_time_ms: 0
      }

      {:ok, result} = KayrockProtocol.parse_response(:api_versions, response)

      assert %ApiVersionsStruct{} = result
      assert result.api_versions[0] == %{min_version: 0, max_version: 8}
    end

    test "dispatches V3 response through parse_response" do
      response = %Kayrock.ApiVersions.V3.Response{
        error_code: 0,
        api_keys: [
          %{api_key: 18, min_version: 0, max_version: 3, tagged_fields: []}
        ],
        throttle_time_ms: 100,
        tagged_fields: []
      }

      {:ok, result} = KayrockProtocol.parse_response(:api_versions, response)

      assert %ApiVersionsStruct{} = result
      assert result.api_versions[18] == %{min_version: 0, max_version: 3}
      assert result.throttle_time_ms == 100
    end
  end

  describe "consistency across versions" do
    test "V0, V1, V2, and V3 produce the same api_versions map for equivalent data" do
      api_keys_plain = [
        %{api_key: 0, min_version: 0, max_version: 8},
        %{api_key: 1, min_version: 0, max_version: 11}
      ]

      api_keys_v3 = [
        %{api_key: 0, min_version: 0, max_version: 8, tagged_fields: []},
        %{api_key: 1, min_version: 0, max_version: 11, tagged_fields: []}
      ]

      v0_response = %Kayrock.ApiVersions.V0.Response{
        error_code: 0,
        api_keys: api_keys_plain
      }

      v1_response = %Kayrock.ApiVersions.V1.Response{
        error_code: 0,
        api_keys: api_keys_plain,
        throttle_time_ms: 50
      }

      v2_response = %Kayrock.ApiVersions.V2.Response{
        error_code: 0,
        api_keys: api_keys_plain,
        throttle_time_ms: 50
      }

      v3_response = %Kayrock.ApiVersions.V3.Response{
        error_code: 0,
        api_keys: api_keys_v3,
        throttle_time_ms: 50,
        tagged_fields: []
      }

      {:ok, result_v0} = ApiVersions.Response.parse_response(v0_response)
      {:ok, result_v1} = ApiVersions.Response.parse_response(v1_response)
      {:ok, result_v2} = ApiVersions.Response.parse_response(v2_response)
      {:ok, result_v3} = ApiVersions.Response.parse_response(v3_response)

      # All versions produce the same api_versions map
      expected_api_versions = %{
        0 => %{min_version: 0, max_version: 8},
        1 => %{min_version: 0, max_version: 11}
      }

      assert result_v0.api_versions == expected_api_versions
      assert result_v1.api_versions == expected_api_versions
      assert result_v2.api_versions == expected_api_versions
      assert result_v3.api_versions == expected_api_versions

      # V0 has nil throttle_time_ms, V1+ have the value
      assert result_v0.throttle_time_ms == nil
      assert result_v1.throttle_time_ms == 50
      assert result_v2.throttle_time_ms == 50
      assert result_v3.throttle_time_ms == 50

      # V1, V2, V3 are fully identical structs
      assert result_v1 == result_v2
      assert result_v2 == result_v3
    end
  end
end
