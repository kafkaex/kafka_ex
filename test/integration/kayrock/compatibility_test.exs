defmodule KafkaEx.KayrockCompatibilityTest do
  @moduledoc """
  These are tests using the original KafkaEx API with the kayrock server
  """

  use ExUnit.Case

  @moduletag :server_kayrock

  alias KafkaEx.ServerKayrock
  alias KafkaEx.Protocol, as: Proto
  alias KafkaEx.Protocol.Offset.Response, as: OffsetResponse

  alias KafkaEx.New.ClusterMetadata
  alias KafkaEx.New.KafkaExAPI

  setup do
    {:ok, args} = KafkaEx.build_worker_options([])

    {:ok, pid} = ServerKayrock.start_link(args)

    {:ok, %{client: pid}}
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
end
