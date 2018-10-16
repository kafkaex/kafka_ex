defmodule KafkaEx.Server0P10P1.Test do
  use ExUnit.Case
  import TestHelper

  @moduletag :server_0_p_10_p_1

  @tag :create_topic
  test "can create a topic" do
    name = "create_topic_#{:rand.uniform(2000000)}"

    request = %{
      topic: name,
      num_partitions: 10,
      replication_factor: 1,
      replica_assignment: [],
      config_entries: [
        %{config_name: "cleanup.policy", config_value: "compact"},
        %{config_name: "min.compaction.lag.ms", config_value: "0"}
      ]}

    resp = KafkaEx.create_topics([request], timeout: 2000)
    assert {:no_error, name} == parse_create_topic_resp(resp)

    resp = KafkaEx.create_topics([request], timeout: 2000)
    assert {:topic_already_exists, name} == parse_create_topic_resp(resp)

    wait_for(fn ->
      topics = KafkaEx.metadata.topic_metadatas |> Enum.map(&(&1.topic))
      assert Enum.member?(topics, name)
    end)
  end

  def parse_create_topic_resp(response) do
    %KafkaEx.Protocol.CreateTopics.Response{
      topic_errors: [
        %KafkaEx.Protocol.CreateTopics.TopicError{
          error_code: error_code,
          topic_name: topic_name
        }
      ]} = response
    {error_code, topic_name}
  end

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

    assert api_versions_kafka_0_10_1_0 == api_versions |> Enum.map(&([&1.api_key, &1.max_version, &1.min_version]))
  end
end
