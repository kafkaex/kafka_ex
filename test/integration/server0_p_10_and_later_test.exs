defmodule KafkaEx.Server0P10P1AndLater.Test do
  use ExUnit.Case
  import TestHelper

  @moduletag :server_0_p_10_and_later

  @tag :create_topic
  test "can create a topic" do
    name = "create_topic_#{:rand.uniform(2_000_000)}"

    config = [
      %{config_name: "cleanup.policy", config_value: "compact"},
      %{config_name: "min.compaction.lag.ms", config_value: "0"}
    ]

    resp = create_topic(name, config)
    assert {:no_error, name} == parse_create_topic_resp(resp)

    resp = create_topic(name, config)
    assert {:topic_already_exists, name} == parse_create_topic_resp(resp)

    wait_for(fn ->
      Enum.member?(existing_topics(), name)
    end)

    assert Enum.member?(existing_topics(), name)
  end

  @tag :delete_topic
  test "can delete a topic" do
    name = "delete_topic_#{:rand.uniform(2_000_000)}"

    resp = create_topic(name, [])
    assert {:no_error, name} == parse_create_topic_resp(resp)

    wait_for(fn ->
      Enum.member?(existing_topics(), name)
    end)

    resp = KafkaEx.delete_topics([name], timeout: 5_000)
    assert {:no_error, name} = parse_delete_topic_resp(resp)

    wait_for(fn ->
      not Enum.member?(existing_topics(), name)
    end)

    assert not Enum.member?(existing_topics(), name)
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

  defp create_topic(name, config) do
    KafkaEx.create_topics(
      [
        %{
          topic: name,
          num_partitions: 10,
          replication_factor: 1,
          replica_assignment: [],
          config_entries: config
        }
      ],
      timeout: 5_000
    )
  end

  defp existing_topics() do
    KafkaEx.metadata().topic_metadatas |> Enum.map(& &1.topic)
  end
end
