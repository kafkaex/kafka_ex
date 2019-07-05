defmodule KafkaEx.Server0P10P1.Test do
  use ExUnit.Case

  @moduletag :server_0_p_10_and_later

  # specific to this server version because we want to test that the api_versions list is exact
  @moduletag :server_0_p_10_p_1

  @tag :api_version
  test "can retrieve api versions" do
    # note this checks against the version of broker we're running in test
    # api_key, max_version, min_version
    api_versions_kafka_0_11_0_1 = [
      [0, 3, 0],
      [1, 5, 0],
      [2, 2, 0],
      [3, 4, 0],
      [4, 0, 0],
      [5, 0, 0],
      [6, 3, 0],
      [7, 1, 1],
      [8, 3, 0],
      [9, 3, 0],
      [10, 1, 0],
      [11, 2, 0],
      [12, 1, 0],
      [13, 1, 0],
      [14, 1, 0],
      [15, 1, 0],
      [16, 1, 0],
      [17, 0, 0],
      [18, 1, 0],
      [19, 2, 0],
      [20, 1, 0],
      [21, 0, 0],
      [22, 0, 0],
      [23, 0, 0],
      [24, 0, 0],
      [25, 0, 0],
      [26, 0, 0],
      [27, 0, 0],
      [28, 0, 0],
      [29, 0, 0],
      [30, 0, 0],
      [31, 0, 0],
      [32, 0, 0],
      [33, 0, 0]
    ]

    response = KafkaEx.api_versions()

    %KafkaEx.Protocol.ApiVersions.Response{
      api_versions: api_versions,
      error_code: :no_error,
      throttle_time_ms: _
    } = response

    assert api_versions_kafka_0_11_0_1 ==
             api_versions
             |> Enum.map(&[&1.api_key, &1.max_version, &1.min_version])
  end
end
