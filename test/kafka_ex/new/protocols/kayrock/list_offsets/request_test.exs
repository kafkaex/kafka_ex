defmodule KafkaEx.New.Protocols.Kayrock.ListOffsets.RequestTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Protocols.Kayrock.ListOffsets.Request, as: ListOffsetsRequest

  alias Kayrock.ListOffsets.V0
  alias Kayrock.ListOffsets.V1
  alias Kayrock.ListOffsets.V2

  @partitions_data [
    %{partition_num: 1, timestamp: -1},
    %{partition_num: 2, timestamp: -1},
    %{partition_num: 3, timestamp: -1}
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
                     %{partition: 1, timestamp: -1, max_num_offsets: 1},
                     %{partition: 2, timestamp: -1, max_num_offsets: 1},
                     %{partition: 3, timestamp: -1, max_num_offsets: 1}
                   ]
                 }
               ]
             }
    end

    test "for v0 - it builds a list offsets request with customs" do
      partitions_data = Enum.map(@partitions_data, &Map.update(&1, :timestamo, 100))
      topics = [{"test_topic", partitions_data}]

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
                     %{partition: 1, timestamp: 100, max_num_offsets: 1},
                     %{partition: 2, timestamp: 100, max_num_offsets: 1},
                     %{partition: 3, timestamp: 100, max_num_offsets: 1}
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
                     %{partition: 1, timestamp: -1},
                     %{partition: 2, timestamp: -1},
                     %{partition: 3, timestamp: -1}
                   ]
                 }
               ]
             }
    end

    test "for v1 - it builds a list offsets request with customs" do
      partitions_data = Enum.map(@partitions_data, &Map.update(&1, :timestamo, 100))
      topics = [{"test_topic", partitions_data}]

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
                     %{partition: 1, timestamp: 100},
                     %{partition: 2, timestamp: 100},
                     %{partition: 3, timestamp: 100}
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
                     %{partition: 1, timestamp: -1},
                     %{partition: 2, timestamp: -1},
                     %{partition: 3, timestamp: -1}
                   ]
                 }
               ]
             }
    end

    test "for v2 - it builds a list offsets request with customs" do
      partitions_data = Enum.map(@partitions_data, &Map.update(&1, :timestamo, 100))
      topics = [{"test_topic", partitions_data}]

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
                     %{partition: 1, timestamp: 100},
                     %{partition: 2, timestamp: 100},
                     %{partition: 3, timestamp: 100}
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
