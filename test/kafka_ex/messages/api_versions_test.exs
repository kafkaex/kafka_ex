defmodule KafkaEx.Messages.ApiVersionsTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Messages.ApiVersions

  describe "build/1" do
    test "builds struct with default empty values" do
      result = ApiVersions.build()

      assert %ApiVersions{} = result
      assert result.api_versions == %{}
      assert result.throttle_time_ms == nil
    end

    test "builds struct with api_versions only" do
      api_versions = %{
        3 => %{min_version: 0, max_version: 2},
        18 => %{min_version: 0, max_version: 1}
      }

      result = ApiVersions.build(api_versions: api_versions)

      assert result.api_versions == api_versions
      assert result.throttle_time_ms == nil
    end

    test "builds struct with both api_versions and throttle_time_ms" do
      api_versions = %{
        0 => %{min_version: 0, max_version: 3},
        1 => %{min_version: 0, max_version: 11}
      }

      result = ApiVersions.build(api_versions: api_versions, throttle_time_ms: 50)

      assert result.api_versions == api_versions
      assert result.throttle_time_ms == 50
    end
  end

  describe "build/1 - edge cases" do
    test "builds struct with throttle_time_ms only" do
      result = ApiVersions.build(throttle_time_ms: 100)

      assert result.api_versions == %{}
      assert result.throttle_time_ms == 100
    end

    test "builds struct with zero throttle_time_ms" do
      result = ApiVersions.build(throttle_time_ms: 0)

      assert result.api_versions == %{}
      assert result.throttle_time_ms == 0
    end

    test "ignores unknown keyword keys" do
      result = ApiVersions.build(unknown_key: "ignored", another: 42)

      assert result.api_versions == %{}
      assert result.throttle_time_ms == nil
    end

    test "build/0 is equivalent to build([])" do
      assert ApiVersions.build() == ApiVersions.build([])
    end

    test "struct can be directly constructed and matches build/1" do
      api_versions = %{0 => %{min_version: 0, max_version: 3}}

      from_build = ApiVersions.build(api_versions: api_versions, throttle_time_ms: 50)
      from_struct = %ApiVersions{api_versions: api_versions, throttle_time_ms: 50}

      assert from_build == from_struct
    end

    test "build with large api_versions map" do
      api_versions =
        for key <- 0..60, into: %{} do
          {key, %{min_version: 0, max_version: key + 1}}
        end

      result = ApiVersions.build(api_versions: api_versions)

      assert map_size(result.api_versions) == 61
    end
  end

  describe "max_version_for_api/2" do
    setup do
      versions =
        ApiVersions.build(
          api_versions: %{
            0 => %{min_version: 0, max_version: 3},
            1 => %{min_version: 0, max_version: 11},
            3 => %{min_version: 0, max_version: 2},
            18 => %{min_version: 0, max_version: 1}
          }
        )

      {:ok, versions: versions}
    end

    test "returns max_version for existing API", %{versions: versions} do
      assert {:ok, 3} = ApiVersions.max_version_for_api(versions, 0)
      assert {:ok, 11} = ApiVersions.max_version_for_api(versions, 1)
      assert {:ok, 2} = ApiVersions.max_version_for_api(versions, 3)
      assert {:ok, 1} = ApiVersions.max_version_for_api(versions, 18)
    end

    test "returns error for non-existent API", %{versions: versions} do
      assert {:error, :unsupported_api} = ApiVersions.max_version_for_api(versions, 999)
      assert {:error, :unsupported_api} = ApiVersions.max_version_for_api(versions, 50)
    end

    test "handles empty api_versions map" do
      versions = ApiVersions.build(api_versions: %{})

      assert {:error, :unsupported_api} = ApiVersions.max_version_for_api(versions, 0)
    end

    test "returns 0 when max_version is 0" do
      versions = ApiVersions.build(api_versions: %{0 => %{min_version: 0, max_version: 0}})

      assert {:ok, 0} = ApiVersions.max_version_for_api(versions, 0)
    end

    test "returns correct value for api_key 0" do
      versions = ApiVersions.build(api_versions: %{0 => %{min_version: 0, max_version: 8}})

      assert {:ok, 8} = ApiVersions.max_version_for_api(versions, 0)
    end
  end

  describe "min_version_for_api/2" do
    setup do
      versions =
        ApiVersions.build(
          api_versions: %{
            0 => %{min_version: 0, max_version: 3},
            1 => %{min_version: 2, max_version: 11},
            3 => %{min_version: 0, max_version: 2},
            18 => %{min_version: 1, max_version: 2}
          }
        )

      {:ok, versions: versions}
    end

    test "returns min_version for existing API", %{versions: versions} do
      assert {:ok, 0} = ApiVersions.min_version_for_api(versions, 0)
      assert {:ok, 2} = ApiVersions.min_version_for_api(versions, 1)
      assert {:ok, 0} = ApiVersions.min_version_for_api(versions, 3)
      assert {:ok, 1} = ApiVersions.min_version_for_api(versions, 18)
    end

    test "returns error for non-existent API", %{versions: versions} do
      assert {:error, :unsupported_api} = ApiVersions.min_version_for_api(versions, 999)
    end

    test "handles empty api_versions map" do
      versions = ApiVersions.build(api_versions: %{})

      assert {:error, :unsupported_api} = ApiVersions.min_version_for_api(versions, 0)
    end

    test "returns 0 when min_version is 0" do
      versions = ApiVersions.build(api_versions: %{0 => %{min_version: 0, max_version: 5}})

      assert {:ok, 0} = ApiVersions.min_version_for_api(versions, 0)
    end

    test "returns non-zero min_version correctly" do
      versions = ApiVersions.build(api_versions: %{18 => %{min_version: 3, max_version: 5}})

      assert {:ok, 3} = ApiVersions.min_version_for_api(versions, 18)
    end

    test "returns error for multiple non-existent APIs" do
      versions = ApiVersions.build(api_versions: %{0 => %{min_version: 0, max_version: 3}})

      assert {:error, :unsupported_api} = ApiVersions.min_version_for_api(versions, 1)
      assert {:error, :unsupported_api} = ApiVersions.min_version_for_api(versions, 999)
    end
  end

  describe "version_supported?/3" do
    setup do
      versions =
        ApiVersions.build(
          api_versions: %{
            3 => %{min_version: 0, max_version: 2},
            18 => %{min_version: 1, max_version: 3},
            50 => %{min_version: 5, max_version: 5}
          }
        )

      {:ok, versions: versions}
    end

    test "returns true for supported versions within range", %{versions: versions} do
      assert ApiVersions.version_supported?(versions, 3, 0) == true
      assert ApiVersions.version_supported?(versions, 3, 1) == true
      assert ApiVersions.version_supported?(versions, 3, 2) == true

      assert ApiVersions.version_supported?(versions, 18, 1) == true
      assert ApiVersions.version_supported?(versions, 18, 2) == true
      assert ApiVersions.version_supported?(versions, 18, 3) == true
    end

    test "returns false for versions below min_version", %{versions: versions} do
      assert ApiVersions.version_supported?(versions, 18, 0) == false
      assert ApiVersions.version_supported?(versions, 50, 4) == false
    end

    test "returns false for versions above max_version", %{versions: versions} do
      assert ApiVersions.version_supported?(versions, 3, 3) == false
      assert ApiVersions.version_supported?(versions, 18, 4) == false
      assert ApiVersions.version_supported?(versions, 50, 6) == false
    end

    test "returns false for absent API key", %{versions: versions} do
      assert ApiVersions.version_supported?(versions, 999, 0) == false
      assert ApiVersions.version_supported?(versions, 100, 1) == false
    end
  end

  describe "version_supported?/3 - boundary and edge cases" do
    test "returns true when version equals min_version exactly" do
      versions = ApiVersions.build(api_versions: %{0 => %{min_version: 2, max_version: 5}})

      assert ApiVersions.version_supported?(versions, 0, 2) == true
    end

    test "returns true when version equals max_version exactly" do
      versions = ApiVersions.build(api_versions: %{0 => %{min_version: 2, max_version: 5}})

      assert ApiVersions.version_supported?(versions, 0, 5) == true
    end

    test "returns false when version is one below min_version" do
      versions = ApiVersions.build(api_versions: %{0 => %{min_version: 2, max_version: 5}})

      assert ApiVersions.version_supported?(versions, 0, 1) == false
    end

    test "returns false when version is one above max_version" do
      versions = ApiVersions.build(api_versions: %{0 => %{min_version: 2, max_version: 5}})

      assert ApiVersions.version_supported?(versions, 0, 6) == false
    end

    test "returns true when min_version equals max_version and version matches" do
      versions = ApiVersions.build(api_versions: %{50 => %{min_version: 5, max_version: 5}})

      assert ApiVersions.version_supported?(versions, 50, 5) == true
    end

    test "returns false when min_version equals max_version and version does not match" do
      versions = ApiVersions.build(api_versions: %{50 => %{min_version: 5, max_version: 5}})

      assert ApiVersions.version_supported?(versions, 50, 4) == false
      assert ApiVersions.version_supported?(versions, 50, 6) == false
    end

    test "returns true for version 0 when min_version is 0" do
      versions = ApiVersions.build(api_versions: %{0 => %{min_version: 0, max_version: 3}})

      assert ApiVersions.version_supported?(versions, 0, 0) == true
    end

    test "returns false for version 0 when min_version is greater than 0" do
      versions = ApiVersions.build(api_versions: %{0 => %{min_version: 1, max_version: 3}})

      assert ApiVersions.version_supported?(versions, 0, 0) == false
    end

    test "handles empty api_versions map" do
      versions = ApiVersions.build(api_versions: %{})

      assert ApiVersions.version_supported?(versions, 0, 0) == false
      assert ApiVersions.version_supported?(versions, 18, 1) == false
    end

    test "returns false for api_key 0 when not in map" do
      versions = ApiVersions.build(api_versions: %{1 => %{min_version: 0, max_version: 3}})

      assert ApiVersions.version_supported?(versions, 0, 0) == false
    end
  end
end
