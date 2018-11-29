defmodule ApiVersionsTest do
  use ExUnit.Case
  @api_versions %{
    0 => %KafkaEx.Protocol.ApiVersions.ApiVersion {
      api_key: 0,
      min_version: 0,
      max_version: 3,
    },
    1 => %KafkaEx.Protocol.ApiVersions.ApiVersion {
      api_key: 1,
      min_version: 7,
      max_version: 8,
    },
    3 => %KafkaEx.Protocol.ApiVersions.ApiVersion {
      api_key: 3,
      min_version: 2,
      max_version: 4,
    }
  }

  test "can correctly determine the adequate api version when api versions is not support" do
    assert {:ok, 1} == KafkaEx.ApiVersions.find_api_version([:unsupported], :metadata, {1, 3})
  end

  test "KafkaEx.ApiVersions can correctly determine the adequate api version when a match exists" do
    assert {:ok, 3} == KafkaEx.ApiVersions.find_api_version(@api_versions, :metadata, {1, 3})
    assert {:ok, 0} == KafkaEx.ApiVersions.find_api_version(@api_versions, :produce, {0, 0})
  end

  test "KafkaEx.ApiVersions replies an error when there's no version match" do
    assert :no_version_supported == KafkaEx.ApiVersions.find_api_version(@api_versions, :fetch, {0, 6})
  end

  test "KafkaEx.ApiVersions replies an error when the api_key is unknown to the server" do
    assert :unknown_message_for_server == KafkaEx.ApiVersions.find_api_version(@api_versions, :create_topics, {0, 1})
  end

  test "KafkaEx.ApiVersions replies an error when the api_key is unknown to the client" do
    assert :unknown_message_for_client == KafkaEx.ApiVersions.find_api_version(@api_versions, :this_does_not_exist, {0, 1})
  end
end

