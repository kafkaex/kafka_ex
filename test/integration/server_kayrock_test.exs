defmodule KafkaEx.ServerKayrock.Test do
  use ExUnit.Case

  alias KafkaEx.ServerKayrock

  alias KafkaEx.New.ClusterMetadata
  alias KafkaEx.New.Topic
  alias KafkaEx.Protocol.Offset.Response, as: OffsetResponse

  @moduletag :server_kayrock

  setup do
    {:ok, args} = KafkaEx.build_worker_options([])

    {:ok, pid} = ServerKayrock.start_link(args)

    {:ok, %{client: pid}}
  end

  test "can update metadata", %{client: client} do
    {:ok, updated_metadata} = ServerKayrock.call(client, :update_metadata)
    %ClusterMetadata{topics: topics} = updated_metadata
    # we don't fetch any topics on startup
    assert topics == %{}

    {:ok, [topic_metadata]} =
      ServerKayrock.call(client, {:topic_metadata, ["test0p8p0"]})

    assert %Topic{name: "test0p8p0"} = topic_metadata
  end

  test "able to list offsets", %{client: client} do
    topic = "test0p8p0"

    for partition <- 0..3 do
      request = %Kayrock.ListOffsets.V1.Request{
        replica_id: -1,
        topics: [
          %{topic: topic, partitions: [%{partition: partition, timestamp: -1}]}
        ]
      }

      {:ok, resp} =
        ServerKayrock.call(
          client,
          {:list_offsets, request}
        )

      %Kayrock.ListOffsets.V1.Response{responses: responses} = resp
      [main_resp] = responses
      [%{error_code: error_code}] = main_resp.partition_responses
      assert error_code == 0
    end
  end

  test "able to list offsets (compatibility)", %{client: client} do
    topic = "test0p8p0"

    {:ok, resp} = KafkaEx.offset(topic, 0, :earliest, client)

    [%OffsetResponse{topic: ^topic, partition_offsets: [partition_offsets]}] =
      resp

    %{error_code: :no_error, offset: [offset], partition: 0} = partition_offsets
    assert offset >= 0
  end
end
