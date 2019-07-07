defmodule KafkaEx.ServerKayrock.Test do
  use ExUnit.Case

  alias KafkaEx.ServerKayrock

  alias KafkaEx.New.ClusterMetadata
  alias KafkaEx.New.KafkaExAPI
  alias KafkaEx.New.Topic

  alias Kayrock.RecordBatch

  @moduletag :server_kayrock

  setup do
    {:ok, args} = KafkaEx.build_worker_options([])

    {:ok, pid} = ServerKayrock.start_link(args)

    {:ok, %{client: pid}}
  end

  test "update metadata", %{client: client} do
    {:ok, updated_metadata} = ServerKayrock.call(client, :update_metadata)
    %ClusterMetadata{topics: topics} = updated_metadata
    # we don't fetch any topics on startup
    assert topics == %{}

    {:ok, [topic_metadata]} =
      ServerKayrock.call(client, {:topic_metadata, ["test0p8p0"]})

    assert %Topic{name: "test0p8p0"} = topic_metadata
  end

  test "list offsets", %{client: client} do
    topic = "test0p8p0"

    for partition <- 0..3 do
      request = %Kayrock.ListOffsets.V1.Request{
        replica_id: -1,
        topics: [
          %{topic: topic, partitions: [%{partition: partition, timestamp: -1}]}
        ]
      }

      {:ok, resp} =
        ServerKayrock.kayrock_call(
          client,
          request,
          {:topic_partition, topic, partition}
        )

      %Kayrock.ListOffsets.V1.Response{responses: responses} = resp
      [main_resp] = responses

      [%{error_code: error_code, offset: offset}] =
        main_resp.partition_responses

      assert error_code == 0
      {:ok, latest_offset} = KafkaExAPI.latest_offset(client, topic, partition)
      assert latest_offset == offset
    end
  end

  test "produce (new message format)", %{client: client} do
    topic = "test0p8p0"
    partition = 1

    {:ok, offset_before} = KafkaExAPI.latest_offset(client, topic, partition)

    record_batch = RecordBatch.from_binary_list(["foo", "bar", "baz"])

    request = %Kayrock.Produce.V1.Request{
      acks: 1,
      timeout: 1000,
      topic_data: [
        %{
          topic: topic,
          data: [
            %{partition: partition, record_set: record_batch}
          ]
        }
      ]
    }

    {:ok, response} =
      ServerKayrock.kayrock_call(
        client,
        request,
        {:topic_partition, topic, partition}
      )

    %Kayrock.Produce.V1.Response{responses: [topic_response]} = response
    assert topic_response.topic == topic

    [%{partition: ^partition, error_code: error_code}] =
      topic_response.partition_responses

    assert error_code == 0

    {:ok, offset_after} = KafkaExAPI.latest_offset(client, topic, partition)
    assert offset_after == offset_before + 3
  end
end
