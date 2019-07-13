defmodule KafkaEx.KayrockCompatibilityTest do
  @moduledoc """
  These are tests using the original KafkaEx API with the kayrock server
  """

  use ExUnit.Case

  @moduletag :server_kayrock

  alias KafkaEx.ServerKayrock
  alias KafkaEx.Protocol, as: Proto
  alias KafkaEx.Protocol.Metadata.Response, as: MetadataResponse
  alias KafkaEx.Protocol.Metadata.Broker
  alias KafkaEx.Protocol.Metadata.TopicMetadata
  alias KafkaEx.Protocol.Offset.Response, as: OffsetResponse

  alias KafkaEx.New.ClusterMetadata
  alias KafkaEx.New.KafkaExAPI

  setup do
    {:ok, args} = KafkaEx.build_worker_options([])

    {:ok, pid} = ServerKayrock.start_link(args, :no_name)

    {:ok, %{client: pid}}
  end

  test "Creates a worker even when the one of the provided brokers is not available" do
    uris = Application.get_env(:kafka_ex, :brokers) ++ [{"bad_host", 9000}]
    {:ok, args} = KafkaEx.build_worker_options(uris: uris)

    {:ok, pid} = ServerKayrock.start_link(args, :no_name)

    assert Process.alive?(pid)
  end

  test "worker updates metadata after specified interval" do
    {:ok, args} = KafkaEx.build_worker_options(metadata_update_interval: 100)
    {:ok, pid} = ServerKayrock.start_link(args, :no_name)
    previous_corr_id = KafkaExAPI.correlation_id(pid)

    :timer.sleep(105)
    curr_corr_id = KafkaExAPI.correlation_id(pid)
    refute curr_corr_id == previous_corr_id
  end

  test "get metadata", %{client: client} do
    metadata = KafkaEx.metadata(worker_name: client, topic: "test0p8p0")

    %MetadataResponse{topic_metadatas: topic_metadatas, brokers: brokers} =
      metadata

    assert is_list(topic_metadatas)
    [topic_metadata | _] = topic_metadatas
    assert %TopicMetadata{} = topic_metadata
    refute topic_metadatas == []
    assert is_list(brokers)
    [broker | _] = brokers
    assert %Broker{} = broker
  end

  test "list offsets", %{client: client} do
    topic = "test0p8p0"

    {:ok, resp} = KafkaEx.offset(topic, 0, :earliest, client)

    [%OffsetResponse{topic: ^topic, partition_offsets: [partition_offsets]}] =
      resp

    %{error_code: :no_error, offset: [offset], partition: 0} = partition_offsets
    assert offset >= 0
  end

  test "produce/4 without an acq required returns :ok", %{client: client} do
    assert KafkaEx.produce("food", 0, "hey",
             worker_name: client,
             required_acks: 0
           ) == :ok
  end

  test "produce/4 with ack required returns an ack", %{client: client} do
    {:ok, offset} =
      KafkaEx.produce(
        "food",
        0,
        "hey",
        worker_name: client,
        required_acks: 1
      )

    assert is_integer(offset)
    refute offset == nil
  end

  test "produce without an acq required returns :ok", %{client: client} do
    assert KafkaEx.produce(
             %Proto.Produce.Request{
               topic: "food",
               partition: 0,
               required_acks: 0,
               messages: [%Proto.Produce.Message{value: "hey"}]
             },
             worker_name: client
           ) == :ok
  end

  test "produce with ack required returns an ack", %{client: client} do
    {:ok, offset} =
      KafkaEx.produce(
        %Proto.Produce.Request{
          topic: "food",
          partition: 0,
          required_acks: 1,
          messages: [%Proto.Produce.Message{value: "hey"}]
        },
        worker_name: client
      )

    refute offset == nil
  end

  test "produce creates log for a non-existing topic", %{client: client} do
    random_string = TestHelper.generate_random_string()

    KafkaEx.produce(
      %Proto.Produce.Request{
        topic: random_string,
        partition: 0,
        required_acks: 1,
        messages: [%Proto.Produce.Message{value: "hey"}]
      },
      worker_name: client
    )

    {:ok, cluster_metadata} = KafkaExAPI.cluster_metadata(client)

    assert random_string in ClusterMetadata.known_topics(cluster_metadata)
  end

  test "fetch returns ':topic_not_found' for non-existing topic", %{
    client: client
  } do
    random_string = TestHelper.generate_random_string()

    assert KafkaEx.fetch(random_string, 0, offset: 0, worker_name: client) ==
             :topic_not_found
  end

  # TODO test fetch empty topic

  test "fetch works", %{client: client} do
    random_string = TestHelper.generate_random_string()

    {:ok, offset} =
      KafkaEx.produce(
        %Proto.Produce.Request{
          topic: random_string,
          partition: 0,
          required_acks: 1,
          messages: [%Proto.Produce.Message{value: "hey foo"}]
        },
        worker_name: client
      )

    fetch_responses =
      KafkaEx.fetch(random_string, 0,
        offset: 0,
        auto_commit: false,
        worker_name: client
      )

    [fetch_response | _] = fetch_responses

    message = fetch_response.partitions |> hd |> Map.get(:message_set) |> hd

    assert message.value == "hey foo"
    assert message.offset == offset
  end
end
