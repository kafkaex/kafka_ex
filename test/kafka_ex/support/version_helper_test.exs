defmodule KafkaEx.Support.VersionHelperTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Support.VersionHelper

  describe "maybe_put_api_version/3" do
    test "adds :api_version to opts when key exists in map" do
      opts = []
      api_versions = %{fetch: 5}
      result = VersionHelper.maybe_put_api_version(opts, api_versions, :fetch)
      assert result == [api_version: 5]
    end

    test "returns opts unchanged when key is absent from map" do
      opts = [some_opt: true]
      api_versions = %{fetch: 5}
      result = VersionHelper.maybe_put_api_version(opts, api_versions, :offset_commit)
      assert result == [some_opt: true]
    end

    test "returns opts unchanged when map is empty" do
      opts = [timeout: 1000]
      result = VersionHelper.maybe_put_api_version(opts, %{}, :fetch)
      assert result == [timeout: 1000]
    end

    test "works with empty opts list" do
      result = VersionHelper.maybe_put_api_version([], %{fetch: 3}, :fetch)
      assert result == [api_version: 3]
    end

    test "preserves existing opts when adding api_version" do
      opts = [timeout: 1000, auto_offset_reset: :earliest]
      api_versions = %{fetch: 7}
      result = VersionHelper.maybe_put_api_version(opts, api_versions, :fetch)
      assert Keyword.get(result, :api_version) == 7
      assert Keyword.get(result, :timeout) == 1000
      assert Keyword.get(result, :auto_offset_reset) == :earliest
    end

    test "version 0 is not treated as nil (falsy check)" do
      opts = []
      api_versions = %{fetch: 0}
      result = VersionHelper.maybe_put_api_version(opts, api_versions, :fetch)
      assert result == [api_version: 0]
    end
  end
end
