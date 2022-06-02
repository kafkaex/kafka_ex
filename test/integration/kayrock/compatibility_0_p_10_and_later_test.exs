defmodule KafkaEx.KayrockCompatibility0p10AndLaterTest do
  @moduledoc """
  These are tests using the original KafkaEx API with the kayrock server

  These come from server0_p_10*_test.exs.  Note that even though Kayrock does
  not support this version of Kafka, the original implementations often delegate
  to previous versions. So unless the test is testing functionality that doesn't
  make sense in newer versions of kafka, we will test it here.
  """

  use ExUnit.Case

  @moduletag :new_client
  @num_partitions 10

  alias KafkaEx.New.KafkaExAPI

  setup do
    {:ok, pid} =
      KafkaEx.start_link_worker(:no_name, server_impl: KafkaEx.New.Client)

    {:ok, %{client: pid}}
  end

  test "can create a topic", %{client: client} do
    name = "create_topic_#{:rand.uniform(2_000_000)}"

    config = [
      %{config_name: "cleanup.policy", config_value: "compact"},
      %{config_name: "min.compaction.lag.ms", config_value: "0"}
    ]

    resp = create_topic(name, config, client)
    assert {:no_error, name} == parse_create_topic_resp(resp)

    resp = create_topic(name, config, client)
    assert {:topic_already_exists, name} == parse_create_topic_resp(resp)

    TestHelper.wait_for(fn ->
      {:ok, metadatas} = KafkaExAPI.topics_metadata(client, [name])
      length(metadatas) > 0
    end)

    {:ok, [metadata]} = KafkaExAPI.topics_metadata(client, [name])
    assert @num_partitions == length(metadata.partitions)
  end

  test "can delete a topic", %{client: client} do
    name = "delete_topic_#{:rand.uniform(2_000_000)}"

    resp = create_topic(name, [], client)
    assert {:no_error, name} == parse_create_topic_resp(resp)

    {:ok, _metadata} = KafkaExAPI.topics_metadata(client, [name])

    resp = KafkaEx.delete_topics([name], timeout: 5_000, worker_name: client)
    assert {:no_error, name} = parse_delete_topic_resp(resp)

    TestHelper.wait_for(fn ->
      {:ok, []} == KafkaExAPI.topics_metadata(client, [name])
    end)
  end

  test "can retrieve api versions", %{client: client} do
    # note this checks against the version of broker we're running in test
    # api_key, max_version, min_version
    api_versions_kafka_0_11_0_1 = [
      [0, 8, 0],
      [1, 11, 0],
      [2, 5, 0],
      [3, 9, 0],
      [4, 4, 0],
      [5, 2, 0],
      [6, 6, 0],
      [7, 3, 0],
      [8, 8, 0],
      [9, 7, 0],
      [10, 3, 0],
      [11, 7, 0],
      [12, 4, 0],
      [13, 4, 0],
      [14, 5, 0],
      [15, 5, 0],
      [16, 3, 0],
      [17, 1, 0],
      [18, 3, 0],
      [19, 5, 0],
      [20, 4, 0],
      [21, 1, 0],
      [22, 3, 0],
      [23, 3, 0],
      [24, 1, 0],
      [25, 1, 0],
      [26, 1, 0],
      [27, 0, 0],
      [28, 3, 0],
      [29, 2, 0],
      [30, 2, 0],
      [31, 2, 0],
      [32, 2, 0],
      [33, 1, 0],
      [34, 1, 0],
      [35, 1, 0],
      [36, 2, 0],
      [37, 2, 0],
      [38, 2, 0],
      [39, 2, 0],
      [40, 2, 0],
      [41, 2, 0],
      [42, 2, 0],
      [43, 2, 0],
      [44, 1, 0],
      [45, 0, 0],
      [46, 0, 0],
      [47, 0, 0]
    ]

    response = KafkaEx.api_versions(worker_name: client)

    %KafkaEx.Protocol.ApiVersions.Response{
      api_versions: api_versions,
      error_code: :no_error,
      throttle_time_ms: _
    } = response

    assert api_versions_kafka_0_11_0_1 ==
             api_versions
             |> Enum.map(&[&1.api_key, &1.max_version, &1.min_version])
  end

  defp parse_create_topic_resp(response) do
    %KafkaEx.Protocol.CreateTopics.Response{
      topic_errors: [
        %KafkaEx.Protocol.CreateTopics.TopicError{
          error_code: error_code,
          topic_name: topic_name
        }
      ]
    } = response

    {error_code, topic_name}
  end

  defp parse_delete_topic_resp(response) do
    %KafkaEx.Protocol.DeleteTopics.Response{
      topic_errors: [
        %KafkaEx.Protocol.DeleteTopics.TopicError{
          error_code: error_code,
          topic_name: topic_name
        }
      ]
    } = response

    {error_code, topic_name}
  end

  defp create_topic(name, config, client) do
    KafkaEx.create_topics(
      [
        %{
          topic: name,
          num_partitions: @num_partitions,
          replication_factor: 1,
          replica_assignment: [],
          config_entries: config
        }
      ],
      timeout: 10_000,
      worker_name: client
    )
  end
end
