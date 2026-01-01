defmodule KafkaEx.Protocol.Kayrock.ListOffsets.RequestTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Protocol.Kayrock.ListOffsets.Request, as: ListOffsetsRequest

  alias Kayrock.ListOffsets.V0
  alias Kayrock.ListOffsets.V1
  alias Kayrock.ListOffsets.V2

  @partitions_data [
    %{partition_num: 1, timestamp: :earliest},
    %{partition_num: 2, timestamp: :latest},
    %{partition_num: 3, timestamp: 123},
    %{partition_num: 4, timestamp: ~U[2024-04-19 12:00:00.000000Z]}
  ]

  describe "build_request/3" do
    test "for v0 - it builds a list offsets request with defaults" do
      topics = [{"test_topic", @partitions_data}]

      request = ListOffsetsRequest.build_request(%V0.Request{}, topics: topics)

      assert request |> attach_client_data() |> V0.Request.serialize()

      assert request == %V0.Request{
               client_id: nil,
               correlation_id: nil,
               replica_id: -1,
               topics: [
                 %{
                   topic: "test_topic",
                   partitions: [
                     %{partition: 1, timestamp: -2, max_num_offsets: 1},
                     %{partition: 2, timestamp: -1, max_num_offsets: 1},
                     %{partition: 3, timestamp: 123, max_num_offsets: 1},
                     %{partition: 4, timestamp: 1_713_528_000_000, max_num_offsets: 1}
                   ]
                 }
               ]
             }
    end

    test "for v0 - it builds a list offsets request with customs" do
      topics = [{"test_topic", @partitions_data}]

      request = ListOffsetsRequest.build_request(%V0.Request{}, topics: topics, replica_id: 2)

      assert request |> attach_client_data() |> V0.Request.serialize()

      assert request == %V0.Request{
               client_id: nil,
               correlation_id: nil,
               replica_id: 2,
               topics: [
                 %{
                   topic: "test_topic",
                   partitions: [
                     %{partition: 1, timestamp: -2, max_num_offsets: 1},
                     %{partition: 2, timestamp: -1, max_num_offsets: 1},
                     %{partition: 3, timestamp: 123, max_num_offsets: 1},
                     %{partition: 4, timestamp: 1_713_528_000_000, max_num_offsets: 1}
                   ]
                 }
               ]
             }
    end

    test "for v1 - it builds a list offsets request with defaults" do
      topics = [{"test_topic", @partitions_data}]

      request = ListOffsetsRequest.build_request(%V1.Request{}, topics: topics)

      assert request |> attach_client_data() |> V1.Request.serialize()

      assert request == %V1.Request{
               client_id: nil,
               correlation_id: nil,
               replica_id: -1,
               topics: [
                 %{
                   topic: "test_topic",
                   partitions: [
                     %{partition: 1, timestamp: -2},
                     %{partition: 2, timestamp: -1},
                     %{partition: 3, timestamp: 123},
                     %{timestamp: 1_713_528_000_000, partition: 4}
                   ]
                 }
               ]
             }
    end

    test "for v1 - it builds a list offsets request with customs" do
      topics = [{"test_topic", @partitions_data}]

      request = ListOffsetsRequest.build_request(%V1.Request{}, topics: topics, replica_id: 2)

      assert request |> attach_client_data() |> V1.Request.serialize()

      assert request == %V1.Request{
               client_id: nil,
               correlation_id: nil,
               replica_id: 2,
               topics: [
                 %{
                   topic: "test_topic",
                   partitions: [
                     %{partition: 1, timestamp: -2},
                     %{partition: 2, timestamp: -1},
                     %{partition: 3, timestamp: 123},
                     %{timestamp: 1_713_528_000_000, partition: 4}
                   ]
                 }
               ]
             }
    end

    test "for v2 - it builds a list offsets request with defaults" do
      topics = [{"test_topic", @partitions_data}]

      request = ListOffsetsRequest.build_request(%V2.Request{}, topics: topics)

      assert request |> attach_client_data() |> V2.Request.serialize()

      assert request == %V2.Request{
               client_id: nil,
               correlation_id: nil,
               replica_id: -1,
               isolation_level: 0,
               topics: [
                 %{
                   topic: "test_topic",
                   partitions: [
                     %{partition: 1, timestamp: -2},
                     %{partition: 2, timestamp: -1},
                     %{partition: 3, timestamp: 123},
                     %{timestamp: 1_713_528_000_000, partition: 4}
                   ]
                 }
               ]
             }
    end

    test "for v2 - it builds a list offsets request with customs" do
      topics = [{"test_topic", @partitions_data}]

      request =
        ListOffsetsRequest.build_request(%V2.Request{},
          topics: topics,
          replica_id: 2,
          isolation_level: :read_commited
        )

      assert request |> attach_client_data() |> V2.Request.serialize()

      assert request == %V2.Request{
               client_id: nil,
               correlation_id: nil,
               replica_id: 2,
               isolation_level: 1,
               topics: [
                 %{
                   topic: "test_topic",
                   partitions: [
                     %{partition: 1, timestamp: -2},
                     %{partition: 2, timestamp: -1},
                     %{partition: 3, timestamp: 123},
                     %{timestamp: 1_713_528_000_000, partition: 4}
                   ]
                 }
               ]
             }
    end
  end

  defp attach_client_data(request) do
    request
    |> Map.put(:client_id, "test")
    |> Map.put(:correlation_id, 123)
  end
end
