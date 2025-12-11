defmodule KafkaEx.New.AdapterApiVersionsTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Adapter
  alias KafkaEx.Protocol.ApiVersions.{ApiVersion, Response}

  describe "Adapter.api_versions/1" do
    test "converts empty api_versions map to legacy format" do
      versions_map = %{}

      response = Adapter.api_versions(versions_map)

      assert %Response{} = response
      assert response.api_versions == []
      assert response.error_code == :no_error
      assert response.throttle_time_ms == 0
    end

    test "converts single API entry to legacy format" do
      versions_map = %{3 => {0, 2}}
      response = Adapter.api_versions(versions_map)

      assert %Response{} = response
      assert length(response.api_versions) == 1
      assert [api_version] = response.api_versions
      assert %ApiVersion{} = api_version
      assert api_version.api_key == 3
      assert api_version.min_version == 0
      assert api_version.max_version == 2
    end

    test "converts multiple API entries to legacy format" do
      versions_map = %{
        0 => {0, 3},
        1 => {0, 11},
        3 => {0, 2},
        18 => {0, 1}
      }

      response = Adapter.api_versions(versions_map)

      assert %Response{} = response
      assert length(response.api_versions) == 4
      assert response.error_code == :no_error
      assert response.throttle_time_ms == 0

      # Check all entries are ApiVersion structs
      Enum.each(response.api_versions, fn api_version ->
        assert %ApiVersion{} = api_version
        assert is_integer(api_version.api_key)
        assert is_integer(api_version.min_version)
        assert is_integer(api_version.max_version)
      end)
    end

    test "sorts API entries by api_key" do
      versions_map = %{
        18 => {0, 1},
        3 => {0, 2},
        0 => {0, 3},
        1 => {0, 11}
      }

      response = Adapter.api_versions(versions_map)

      api_keys = Enum.map(response.api_versions, & &1.api_key)
      assert api_keys == [0, 1, 3, 18]
    end

    test "preserves min and max versions correctly" do
      versions_map = %{
        0 => {0, 3},
        1 => {2, 11},
        3 => {1, 5},
        18 => {0, 2}
      }

      response = Adapter.api_versions(versions_map)

      # Check Produce API (key 0)
      produce = Enum.find(response.api_versions, &(&1.api_key == 0))
      assert produce.min_version == 0
      assert produce.max_version == 3

      # Check Fetch API (key 1)
      fetch = Enum.find(response.api_versions, &(&1.api_key == 1))
      assert fetch.min_version == 2
      assert fetch.max_version == 11

      # Check Metadata API (key 3)
      metadata = Enum.find(response.api_versions, &(&1.api_key == 3))
      assert metadata.min_version == 1
      assert metadata.max_version == 5

      # Check ApiVersions API (key 18)
      api_versions = Enum.find(response.api_versions, &(&1.api_key == 18))
      assert api_versions.min_version == 0
      assert api_versions.max_version == 2
    end

    test "always returns :no_error for error_code" do
      versions_map = %{0 => {0, 1}}

      response = Adapter.api_versions(versions_map)

      assert response.error_code == :no_error
    end

    test "always returns 0 for throttle_time_ms" do
      versions_map = %{0 => {0, 1}}

      response = Adapter.api_versions(versions_map)

      assert response.throttle_time_ms == 0
    end

    test "handles large number of API entries" do
      # Create a map with many API entries (simulating a real Kafka broker)
      versions_map =
        Enum.into(0..50, %{}, fn api_key ->
          {api_key, {0, api_key + 1}}
        end)

      response = Adapter.api_versions(versions_map)

      assert length(response.api_versions) == 51
      # Verify sorting
      api_keys = Enum.map(response.api_versions, & &1.api_key)
      assert api_keys == Enum.to_list(0..50)
    end

    test "handles APIs with same min and max version" do
      versions_map = %{
        50 => {5, 5}
      }

      response = Adapter.api_versions(versions_map)

      [api_version] = response.api_versions
      assert api_version.api_key == 50
      assert api_version.min_version == 5
      assert api_version.max_version == 5
    end
  end
end
