defmodule KafkaEx.Protocol.Kayrock.ApiVersions.ResponseHelpersTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Protocol.Kayrock.ApiVersions.ResponseHelpers
  alias KafkaEx.Messages.ApiVersions
  alias KafkaEx.Client.Error

  # ---------------------------------------------------------------------------
  # parse/1 — V1+ responses (with throttle_time_ms)
  # ---------------------------------------------------------------------------

  describe "parse/1 - success (error_code: 0)" do
    test "returns {:ok, %ApiVersions{}} with api_versions map and throttle_time_ms" do
      input = %{
        error_code: 0,
        api_keys: [
          %{api_key: 0, min_version: 0, max_version: 8},
          %{api_key: 1, min_version: 0, max_version: 11}
        ],
        throttle_time_ms: 100
      }

      assert {:ok, %ApiVersions{} = result} = ResponseHelpers.parse(input)

      assert result.api_versions == %{
               0 => %{min_version: 0, max_version: 8},
               1 => %{min_version: 0, max_version: 11}
             }

      assert result.throttle_time_ms == 100
    end

    test "handles empty api_keys list" do
      input = %{error_code: 0, api_keys: [], throttle_time_ms: 0}

      assert {:ok, %ApiVersions{} = result} = ResponseHelpers.parse(input)
      assert result.api_versions == %{}
      assert result.throttle_time_ms == 0
    end

    test "handles single api_key entry" do
      input = %{
        error_code: 0,
        api_keys: [%{api_key: 18, min_version: 0, max_version: 3}],
        throttle_time_ms: 50
      }

      assert {:ok, %ApiVersions{} = result} = ResponseHelpers.parse(input)
      assert result.api_versions == %{18 => %{min_version: 0, max_version: 3}}
    end

    test "handles zero throttle_time_ms" do
      input = %{
        error_code: 0,
        api_keys: [%{api_key: 3, min_version: 0, max_version: 2}],
        throttle_time_ms: 0
      }

      assert {:ok, %ApiVersions{} = result} = ResponseHelpers.parse(input)
      assert result.throttle_time_ms == 0
    end

    test "handles large throttle_time_ms value" do
      input = %{
        error_code: 0,
        api_keys: [%{api_key: 0, min_version: 0, max_version: 1}],
        throttle_time_ms: 2_147_483_647
      }

      assert {:ok, %ApiVersions{} = result} = ResponseHelpers.parse(input)
      assert result.throttle_time_ms == 2_147_483_647
    end

    test "accepts entries with extra keys (like tagged_fields from V3)" do
      input = %{
        error_code: 0,
        api_keys: [
          %{api_key: 0, min_version: 0, max_version: 8, tagged_fields: [{42, <<1, 2>>}]},
          %{api_key: 1, min_version: 0, max_version: 11, tagged_fields: []}
        ],
        throttle_time_ms: 0
      }

      # build_api_versions_map only extracts api_key, min_version, max_version
      assert {:ok, %ApiVersions{} = result} = ResponseHelpers.parse(input)
      assert result.api_versions[0] == %{min_version: 0, max_version: 8}
      assert result.api_versions[1] == %{min_version: 0, max_version: 11}
    end
  end

  describe "parse/1 - error (error_code != 0)" do
    test "returns {:error, %Error{}} for unsupported_version (35)" do
      input = %{error_code: 35, api_keys: [], throttle_time_ms: 0}

      assert {:error, %Error{} = error} = ResponseHelpers.parse(input)
      assert error.error == :unsupported_version
    end

    test "returns {:error, %Error{}} for unknown_server_error (-1)" do
      input = %{error_code: -1, api_keys: [], throttle_time_ms: 0}

      assert {:error, %Error{} = error} = ResponseHelpers.parse(input)
      assert error.error == :unknown_server_error
    end

    test "returns {:error, %Error{}} for unsupported_sasl_mechanism (33)" do
      input = %{error_code: 33, api_keys: [], throttle_time_ms: 0}

      assert {:error, %Error{} = error} = ResponseHelpers.parse(input)
      assert error.error == :unsupported_sasl_mechanism
    end

    test "returns {:error, %Error{}} for offset_out_of_range (1)" do
      input = %{error_code: 1, api_keys: [], throttle_time_ms: 0}

      assert {:error, %Error{} = error} = ResponseHelpers.parse(input)
      assert error.error == :offset_out_of_range
    end

    test "error response has empty metadata" do
      input = %{error_code: 35, api_keys: [], throttle_time_ms: 100}

      assert {:error, %Error{} = error} = ResponseHelpers.parse(input)
      assert error.metadata == %{}
    end
  end

  # ---------------------------------------------------------------------------
  # parse_v0/1 — V0 responses (no throttle_time_ms)
  # ---------------------------------------------------------------------------

  describe "parse_v0/1 - success (error_code: 0)" do
    test "returns {:ok, %ApiVersions{}} with api_versions map and nil throttle_time_ms" do
      input = %{
        error_code: 0,
        api_keys: [
          %{api_key: 0, min_version: 0, max_version: 3},
          %{api_key: 18, min_version: 0, max_version: 1}
        ]
      }

      assert {:ok, %ApiVersions{} = result} = ResponseHelpers.parse_v0(input)

      assert result.api_versions == %{
               0 => %{min_version: 0, max_version: 3},
               18 => %{min_version: 0, max_version: 1}
             }

      assert result.throttle_time_ms == nil
    end

    test "handles empty api_keys list" do
      input = %{error_code: 0, api_keys: []}

      assert {:ok, %ApiVersions{} = result} = ResponseHelpers.parse_v0(input)
      assert result.api_versions == %{}
      assert result.throttle_time_ms == nil
    end

    test "handles single api_key entry" do
      input = %{
        error_code: 0,
        api_keys: [%{api_key: 3, min_version: 0, max_version: 9}]
      }

      assert {:ok, %ApiVersions{} = result} = ResponseHelpers.parse_v0(input)
      assert result.api_versions == %{3 => %{min_version: 0, max_version: 9}}
    end

    test "always sets throttle_time_ms to nil regardless of input" do
      # V0 parse function does not read throttle_time_ms from the input at all
      input = %{error_code: 0, api_keys: []}

      assert {:ok, %ApiVersions{} = result} = ResponseHelpers.parse_v0(input)
      assert result.throttle_time_ms == nil
    end
  end

  describe "parse_v0/1 - error (error_code != 0)" do
    test "returns {:error, %Error{}} for unsupported_version (35)" do
      input = %{error_code: 35, api_keys: []}

      assert {:error, %Error{} = error} = ResponseHelpers.parse_v0(input)
      assert error.error == :unsupported_version
    end

    test "returns {:error, %Error{}} for unknown_server_error (-1)" do
      input = %{error_code: -1, api_keys: []}

      assert {:error, %Error{} = error} = ResponseHelpers.parse_v0(input)
      assert error.error == :unknown_server_error
    end

    test "error path ignores api_keys content" do
      # When error_code != 0, the api_keys should not matter since
      # the guard `when error_code != 0` matches first
      input = %{
        error_code: 35,
        api_keys: [%{api_key: 0, min_version: 0, max_version: 8}]
      }

      assert {:error, %Error{}} = ResponseHelpers.parse_v0(input)
    end
  end

  # ---------------------------------------------------------------------------
  # build_api_versions_map/1 — direct tests
  # ---------------------------------------------------------------------------

  describe "build_api_versions_map/1" do
    test "converts list of entries to map keyed by api_key" do
      entries = [
        %{api_key: 0, min_version: 0, max_version: 3},
        %{api_key: 1, min_version: 0, max_version: 11},
        %{api_key: 18, min_version: 0, max_version: 3}
      ]

      result = ResponseHelpers.build_api_versions_map(entries)

      assert result == %{
               0 => %{min_version: 0, max_version: 3},
               1 => %{min_version: 0, max_version: 11},
               18 => %{min_version: 0, max_version: 3}
             }
    end

    test "returns empty map for empty list" do
      assert ResponseHelpers.build_api_versions_map([]) == %{}
    end

    test "handles single entry" do
      entries = [%{api_key: 18, min_version: 0, max_version: 3}]

      result = ResponseHelpers.build_api_versions_map(entries)

      assert result == %{18 => %{min_version: 0, max_version: 3}}
    end

    test "duplicate api_key entries — last entry wins (Enum.into semantics)" do
      entries = [
        %{api_key: 0, min_version: 0, max_version: 3},
        %{api_key: 0, min_version: 2, max_version: 8}
      ]

      result = ResponseHelpers.build_api_versions_map(entries)

      # Enum.into processes in order; later entries overwrite earlier ones
      assert result == %{0 => %{min_version: 2, max_version: 8}}
      assert map_size(result) == 1
    end

    test "extracts only min_version and max_version, ignoring extra keys" do
      entries = [
        %{api_key: 0, min_version: 0, max_version: 8, tagged_fields: [{42, <<1>>}], extra: "ignored"}
      ]

      result = ResponseHelpers.build_api_versions_map(entries)

      # The function only extracts min_version and max_version
      assert result == %{0 => %{min_version: 0, max_version: 8}}
    end

    test "handles large number of entries" do
      entries =
        for api_key <- 0..60 do
          %{api_key: api_key, min_version: 0, max_version: api_key + 1}
        end

      result = ResponseHelpers.build_api_versions_map(entries)

      assert map_size(result) == 61
      assert result[0] == %{min_version: 0, max_version: 1}
      assert result[60] == %{min_version: 0, max_version: 61}
    end

    test "handles entries where min_version equals max_version" do
      entries = [%{api_key: 50, min_version: 5, max_version: 5}]

      result = ResponseHelpers.build_api_versions_map(entries)

      assert result == %{50 => %{min_version: 5, max_version: 5}}
    end

    test "handles entries with high api_key numbers" do
      entries = [%{api_key: 999, min_version: 0, max_version: 1}]

      result = ResponseHelpers.build_api_versions_map(entries)

      assert result == %{999 => %{min_version: 0, max_version: 1}}
    end
  end
end
