defmodule KafkaEx.KayrockCompatibility0p10AndLaterTest do
  @moduledoc """
  These are tests using the original KafkaEx API with the kayrock server

  These come from server0_p_10*_test.exs.  Note that even though Kayrock does
  not support this version of Kafka, the original implementations often delegate
  to previous versions. So unless the test is testing functionality that doesn't
  make sense in newer versions of kafka, we will test it here.
  """

  use ExUnit.Case

  @moduletag :server_kayrock
  @num_partitions 10

  alias KafkaEx.ServerKayrock
  alias KafkaEx.New.KafkaExAPI

  setup do
    {:ok, args} = KafkaEx.build_worker_options([])

    {:ok, pid} = ServerKayrock.start_link(args, :no_name)

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

    {:ok, [metadata]} = KafkaExAPI.topics_metadata(client, [name])
    assert @num_partitions == length(metadata.partitions)
  end

  test "can delete a topic", %{client: client} do
    name = "delete_topic_#{:rand.uniform(2_000_000)}"

    resp = create_topic(name, [], client)
    assert {:no_error, name} == parse_create_topic_resp(resp)

    {:ok, [_metadata]} = KafkaExAPI.topics_metadata(client, [name])

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
