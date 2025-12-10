defmodule KafkaEx.Server0P10P1.Test do
  use ExUnit.Case

  @moduletag :server_0_p_10_and_later

  # specific to this server version because we want to test that the api_versions list is exact
  @moduletag :server_0_p_10_p_1

  @tag :api_version
  test "can retrieve api versions" do
    # note this checks against the version of broker we're running in test
    # Updated for Kafka 2.5 (Confluent Platform 5.5.1)
    # api_key, max_version, min_version
    api_versions_kafka_2_5 = [
      [0, 9, 0],
      [1, 12, 0],
      [2, 7, 0],
      [3, 11, 0],
      [4, 5, 0],
      [5, 3, 0],
      [6, 7, 0],
      [7, 3, 0],
      [8, 8, 0],
      [9, 8, 0],
      [10, 4, 0],
      [11, 7, 0],
      [12, 4, 0],
      [13, 4, 0],
      [14, 5, 0],
      [15, 5, 0],
      [16, 4, 0],
      [17, 1, 0],
      [18, 3, 0],
      [19, 7, 0],
      [20, 6, 0],
      [21, 2, 0],
      [22, 4, 0],
      [23, 4, 0],
      [24, 3, 0],
      [25, 3, 0],
      [26, 3, 0],
      [27, 1, 0],
      [28, 3, 0],
      [29, 2, 0],
      [30, 2, 0],
      [31, 2, 0],
      [32, 4, 0],
      [33, 2, 0],
      [34, 2, 0],
      [35, 2, 0],
      [36, 2, 0],
      [37, 3, 0],
      [38, 2, 0],
      [39, 2, 0],
      [40, 2, 0],
      [41, 2, 0],
      [42, 2, 0],
      [43, 2, 0],
      [44, 1, 0],
      [45, 0, 0],
      [46, 0, 0],
      [47, 0, 0],
      [48, 1, 0],
      [49, 1, 0],
      [50, 0, 0],
      [51, 0, 0],
      [56, 0, 0],
      [57, 0, 0],
      [60, 0, 0],
      [61, 0, 0],
      [65, 0, 0],
      [66, 0, 0],
      [67, 0, 0]
    ]

    response = KafkaEx.api_versions()

    %KafkaEx.Protocol.ApiVersions.Response{
      api_versions: api_versions,
      error_code: :no_error,
      throttle_time_ms: _
    } = response

    assert api_versions_kafka_2_5 ==
             api_versions
             |> Enum.map(&[&1.api_key, &1.max_version, &1.min_version])
  end
end
