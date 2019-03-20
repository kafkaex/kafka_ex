defmodule KafkaEx.Server0P10P1.Test do
  use ExUnit.Case

  @moduletag :server_0_p_10_and_later

  # specific to this server version because we want to test that the api_versions list is exact
  @moduletag :server_0_p_10_p_1

  @tag :api_version
  test "can retrieve api versions" do
    # api_key, max_version, min_version
    api_versions_kafka_0_10_1_0 = [
      [0, 2, 0],
      [1, 3, 0],
      [2, 1, 0],
      [3, 2, 0],
      [4, 0, 0],
      [5, 0, 0],
      [6, 2, 0],
      [7, 1, 1],
      [8, 2, 0],
      [9, 1, 0],
      [10, 0, 0],
      [11, 1, 0],
      [12, 0, 0],
      [13, 0, 0],
      [14, 0, 0],
      [15, 0, 0],
      [16, 0, 0],
      [17, 0, 0],
      [18, 0, 0],
      [19, 0, 0],
      [20, 0, 0]
    ]

    response = KafkaEx.api_versions()

    %KafkaEx.Protocol.ApiVersions.Response{
      api_versions: api_versions,
      error_code: :no_error,
      throttle_time_ms: _
    } = response

    assert api_versions_kafka_0_10_1_0 ==
             api_versions
             |> Enum.map(&[&1.api_key, &1.max_version, &1.min_version])
  end
end
