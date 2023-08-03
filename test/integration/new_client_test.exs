defmodule KafkaEx.New.Client.Test do
  use ExUnit.Case

  alias KafkaEx.New.Client

  alias KafkaEx.New.Structs.ClusterMetadata
  alias KafkaEx.New.KafkaExAPI
  alias KafkaEx.New.Structs.Topic
  alias KafkaEx.New.Structs.NodeSelector

  alias Kayrock.RecordBatch
  alias Kayrock.RecordBatch.Record
  alias Kayrock.RecordBatch.RecordHeader

  @moduletag :new_client

  setup do
    {:ok, args} = KafkaEx.build_worker_options([])

    {:ok, pid} = Client.start_link(args, :no_name)

    {:ok, %{client: pid}}
  end

  test "update metadata", %{client: client} do
    {:ok, updated_metadata} = GenServer.call(client, :update_metadata)
    %ClusterMetadata{topics: topics} = updated_metadata
    # we don't fetch any topics on startup
    assert topics == %{}

    {:ok, [topic_metadata]} =
      GenServer.call(client, {:topic_metadata, ["test0p8p0"], false})

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
        Client.send_request(
          client,
          request,
          NodeSelector.topic_partition(topic, partition)
        )

      %Kayrock.ListOffsets.V1.Response{responses: [main_resp]} = resp

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
      acks: -1,
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
      Client.send_request(
        client,
        request,
        NodeSelector.topic_partition(topic, partition)
      )

    %Kayrock.Produce.V1.Response{responses: [topic_response]} = response
    assert topic_response.topic == topic

    [%{partition: ^partition, error_code: error_code}] =
      topic_response.partition_responses

    assert error_code == 0

    {:ok, offset_after} = KafkaExAPI.latest_offset(client, topic, partition)
    assert offset_after == offset_before + 3
  end

  test "produce with record headers (new message format)", %{client: client} do
    topic = "test0p8p0"
    partition = 1

    {:ok, offset_before} = KafkaExAPI.latest_offset(client, topic, partition)

    headers = [
      %RecordHeader{key: "source", value: "System-X"},
      %RecordHeader{key: "type", value: "HeaderCreatedEvent"}
    ]

    records = [
      %Record{
        headers: headers,
        key: "key-0001",
        value: "msg value for key 0001"
      }
    ]

    record_batch = %RecordBatch{
      attributes: 0,
      records: records
    }

    request = %Kayrock.Produce.V1.Request{
      acks: -1,
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
      Client.send_request(
        client,
        request,
        NodeSelector.topic_partition(topic, partition)
      )

    %Kayrock.Produce.V1.Response{responses: [topic_response]} = response
    assert topic_response.topic == topic

    [%{partition: ^partition, error_code: error_code}] =
      topic_response.partition_responses

    assert error_code == 0

    {:ok, offset_after} = KafkaExAPI.latest_offset(client, topic, partition)
    assert offset_after == offset_before + 1
  end

  test "client can receive {:ssl_closed, _}", %{client: client} do
    send(client, {:ssl_closed, :unused})

    TestHelper.wait_for(fn ->
      {:message_queue_len, m} = Process.info(client, :message_queue_len)
      m == 0
    end)

    assert Process.alive?(client)
  end

  test "client can receive {:tcp_closed, _}", %{client: client} do
    send(client, {:tcp_closed, :unused})

    TestHelper.wait_for(fn ->
      {:message_queue_len, m} = Process.info(client, :message_queue_len)
      m == 0
    end)

    assert Process.alive?(client)
  end
end
