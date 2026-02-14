defmodule KafkaEx.Protocol.Kayrock.ListOffsets.RequestTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Protocol.Kayrock.ListOffsets.Request, as: ListOffsetsRequest

  alias Kayrock.ListOffsets.V0
  alias Kayrock.ListOffsets.V1
  alias Kayrock.ListOffsets.V2
  alias Kayrock.ListOffsets.V3
  alias Kayrock.ListOffsets.V4
  alias Kayrock.ListOffsets.V5

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

    # -------------------------------------------------------------------------
    # V3 Request: identical Kayrock schema to V2 (no current_leader_epoch)
    # -------------------------------------------------------------------------

    test "for v3 - it builds a list offsets request with defaults" do
      topics = [{"test_topic", @partitions_data}]

      request = ListOffsetsRequest.build_request(%V3.Request{}, topics: topics)

      assert request |> attach_client_data() |> V3.Request.serialize()

      assert request == %V3.Request{
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
                     %{partition: 4, timestamp: 1_713_528_000_000}
                   ]
                 }
               ]
             }
    end

    test "for v3 - it builds a list offsets request with customs" do
      topics = [{"test_topic", @partitions_data}]

      request =
        ListOffsetsRequest.build_request(%V3.Request{},
          topics: topics,
          replica_id: 3,
          isolation_level: :read_commited
        )

      assert request |> attach_client_data() |> V3.Request.serialize()

      assert request == %V3.Request{
               client_id: nil,
               correlation_id: nil,
               replica_id: 3,
               isolation_level: 1,
               topics: [
                 %{
                   topic: "test_topic",
                   partitions: [
                     %{partition: 1, timestamp: -2},
                     %{partition: 2, timestamp: -1},
                     %{partition: 3, timestamp: 123},
                     %{partition: 4, timestamp: 1_713_528_000_000}
                   ]
                 }
               ]
             }
    end

    test "for v3 - partitions do NOT include current_leader_epoch" do
      topics = [{"test_topic", [%{partition_num: 0, timestamp: :latest}]}]

      request =
        ListOffsetsRequest.build_request(%V3.Request{},
          topics: topics,
          current_leader_epoch: 7
        )

      assert [%{partitions: [partition]}] = request.topics
      refute Map.has_key?(partition, :current_leader_epoch)
    end

    # -------------------------------------------------------------------------
    # V4 Request: adds current_leader_epoch in partitions
    # -------------------------------------------------------------------------

    test "for v4 - it builds a list offsets request with defaults" do
      topics = [{"test_topic", @partitions_data}]

      request = ListOffsetsRequest.build_request(%V4.Request{}, topics: topics)

      assert request |> attach_client_data() |> V4.Request.serialize()

      assert request == %V4.Request{
               client_id: nil,
               correlation_id: nil,
               replica_id: -1,
               isolation_level: 0,
               topics: [
                 %{
                   topic: "test_topic",
                   partitions: [
                     %{partition: 1, timestamp: -2, current_leader_epoch: -1},
                     %{partition: 2, timestamp: -1, current_leader_epoch: -1},
                     %{partition: 3, timestamp: 123, current_leader_epoch: -1},
                     %{partition: 4, timestamp: 1_713_528_000_000, current_leader_epoch: -1}
                   ]
                 }
               ]
             }
    end

    test "for v4 - it builds a list offsets request with customs" do
      topics = [{"test_topic", @partitions_data}]

      request =
        ListOffsetsRequest.build_request(%V4.Request{},
          topics: topics,
          replica_id: 4,
          isolation_level: :read_commited,
          current_leader_epoch: 12
        )

      assert request |> attach_client_data() |> V4.Request.serialize()

      assert request == %V4.Request{
               client_id: nil,
               correlation_id: nil,
               replica_id: 4,
               isolation_level: 1,
               topics: [
                 %{
                   topic: "test_topic",
                   partitions: [
                     %{partition: 1, timestamp: -2, current_leader_epoch: 12},
                     %{partition: 2, timestamp: -1, current_leader_epoch: 12},
                     %{partition: 3, timestamp: 123, current_leader_epoch: 12},
                     %{partition: 4, timestamp: 1_713_528_000_000, current_leader_epoch: 12}
                   ]
                 }
               ]
             }
    end

    test "for v4 - partitions include current_leader_epoch with default -1" do
      topics = [{"test_topic", [%{partition_num: 0, timestamp: :latest}]}]

      request = ListOffsetsRequest.build_request(%V4.Request{}, topics: topics)

      assert [%{partitions: [partition]}] = request.topics
      assert partition.current_leader_epoch == -1
    end

    # -------------------------------------------------------------------------
    # V5 Request: identical to V4
    # -------------------------------------------------------------------------

    test "for v5 - it builds a list offsets request with defaults (same as V4)" do
      topics = [{"test_topic", @partitions_data}]

      request = ListOffsetsRequest.build_request(%V5.Request{}, topics: topics)

      assert request |> attach_client_data() |> V5.Request.serialize()

      assert request == %V5.Request{
               client_id: nil,
               correlation_id: nil,
               replica_id: -1,
               isolation_level: 0,
               topics: [
                 %{
                   topic: "test_topic",
                   partitions: [
                     %{partition: 1, timestamp: -2, current_leader_epoch: -1},
                     %{partition: 2, timestamp: -1, current_leader_epoch: -1},
                     %{partition: 3, timestamp: 123, current_leader_epoch: -1},
                     %{partition: 4, timestamp: 1_713_528_000_000, current_leader_epoch: -1}
                   ]
                 }
               ]
             }
    end

    test "for v5 - it builds a list offsets request with customs" do
      topics = [{"test_topic", @partitions_data}]

      request =
        ListOffsetsRequest.build_request(%V5.Request{},
          topics: topics,
          replica_id: 5,
          isolation_level: :read_commited,
          current_leader_epoch: 99
        )

      assert request |> attach_client_data() |> V5.Request.serialize()

      assert request == %V5.Request{
               client_id: nil,
               correlation_id: nil,
               replica_id: 5,
               isolation_level: 1,
               topics: [
                 %{
                   topic: "test_topic",
                   partitions: [
                     %{partition: 1, timestamp: -2, current_leader_epoch: 99},
                     %{partition: 2, timestamp: -1, current_leader_epoch: 99},
                     %{partition: 3, timestamp: 123, current_leader_epoch: 99},
                     %{partition: 4, timestamp: 1_713_528_000_000, current_leader_epoch: 99}
                   ]
                 }
               ]
             }
    end

    test "for v5 - partitions include current_leader_epoch" do
      topics = [{"test_topic", [%{partition_num: 0, timestamp: :earliest}]}]

      request =
        ListOffsetsRequest.build_request(%V5.Request{},
          topics: topics,
          current_leader_epoch: 42
        )

      assert [%{partitions: [partition]}] = request.topics
      assert partition.current_leader_epoch == 42
    end

    # -------------------------------------------------------------------------
    # V2 Regression: V2 was refactored to use RequestHelpers.build_request_v2_plus/3
    # Verify it still produces serializable output identical to V2 Kayrock struct
    # -------------------------------------------------------------------------

    test "for v2 - regression: serialized binary matches expected wire format" do
      topics = [{"t", [%{partition_num: 0, timestamp: :latest}]}]

      request =
        ListOffsetsRequest.build_request(%V2.Request{}, topics: topics)
        |> attach_client_data()

      # Verify it serializes without error and produces the expected binary
      binary = request |> V2.Request.serialize() |> IO.iodata_to_binary()

      # Manually construct expected binary:
      # Header: api_key(2)::16, api_vsn(2)::16, correlation_id(123)::32, client_id_len(4)::16, "test"
      # Body: replica_id(-1)::32, isolation_level(0)::8,
      #   topics_count(1)::32, topic_len(1)::16, "t",
      #   partitions_count(1)::32, partition(0)::32, timestamp(-1)::64
      expected =
        <<2::16, 2::16, 123::32, 4::16, "test"::binary, -1::32-signed, 0::8, 1::32-signed, 1::16, "t"::binary,
          1::32-signed, 0::32-signed, -1::64-signed>>

      assert binary == expected
    end
  end

  # ---------------------------------------------------------------------------
  # Any fallback implementation tests (plain maps -- no __struct__ key)
  # ---------------------------------------------------------------------------

  describe "Any fallback implementation — plain map V0-V1 path (no isolation_level)" do
    test "plain map without isolation_level takes V0-V1 path, defaults to V1" do
      # Plain map (no __struct__) triggers Any fallback.
      # No isolation_level key means V0-V1 branch.
      # Default api_version is 1, so no max_num_offsets.
      plain_map = %{replica_id: nil, topics: []}

      topics = [{"test_topic", [%{partition_num: 0, timestamp: :latest}]}]

      result = ListOffsetsRequest.build_request(plain_map, topics: topics)

      assert result.replica_id == -1
      assert [%{topic: "test_topic", partitions: [partition]}] = result.topics
      assert partition == %{partition: 0, timestamp: -1}
      refute Map.has_key?(partition, :max_num_offsets)
    end

    test "plain map with explicit api_version: 0 takes V0 path with max_num_offsets" do
      plain_map = %{replica_id: nil, topics: []}

      topics = [{"test_topic", [%{partition_num: 0, timestamp: :earliest}]}]

      result = ListOffsetsRequest.build_request(plain_map, topics: topics, api_version: 0)

      assert result.replica_id == -1
      assert [%{topic: "test_topic", partitions: [partition]}] = result.topics
      assert partition.partition == 0
      assert partition.timestamp == -2
      assert partition.max_num_offsets == 1
    end

    test "plain map V0 path respects custom offset_num" do
      plain_map = %{replica_id: nil, topics: []}

      topics = [{"t", [%{partition_num: 0, timestamp: :latest}]}]

      result =
        ListOffsetsRequest.build_request(plain_map,
          topics: topics,
          api_version: 0,
          offset_num: 5
        )

      assert [%{partitions: [partition]}] = result.topics
      assert partition.max_num_offsets == 5
    end

    test "plain map V0-V1 path respects custom replica_id" do
      plain_map = %{replica_id: nil, topics: []}

      topics = [{"t", [%{partition_num: 0, timestamp: :latest}]}]

      result = ListOffsetsRequest.build_request(plain_map, topics: topics, replica_id: 7)

      assert result.replica_id == 7
    end

    test "plain map V1 path handles DateTime timestamp" do
      plain_map = %{replica_id: nil, topics: []}

      topics = [{"t", [%{partition_num: 0, timestamp: ~U[2024-04-19 12:00:00.000000Z]}]}]

      result = ListOffsetsRequest.build_request(plain_map, topics: topics)

      assert [%{partitions: [partition]}] = result.topics
      assert partition.timestamp == 1_713_528_000_000
    end
  end

  describe "Any fallback implementation — plain map V2+ path (has isolation_level)" do
    test "plain map with isolation_level defaults to V2 (no current_leader_epoch)" do
      # isolation_level present triggers V2+ branch.
      # No explicit api_version => defaults to 2 (safe baseline).
      plain_map = %{isolation_level: nil, replica_id: nil, topics: []}

      topics = [{"test_topic", [%{partition_num: 0, timestamp: :latest}]}]

      result = ListOffsetsRequest.build_request(plain_map, topics: topics)

      assert result.replica_id == -1
      assert result.isolation_level == 0
      assert [%{topic: "test_topic", partitions: [partition]}] = result.topics
      assert partition == %{partition: 0, timestamp: -1}
      refute Map.has_key?(partition, :current_leader_epoch)
    end

    test "plain map with isolation_level and explicit api_version: 4 adds current_leader_epoch" do
      plain_map = %{isolation_level: nil, replica_id: nil, topics: []}

      topics = [{"test_topic", [%{partition_num: 0, timestamp: :latest}]}]

      result =
        ListOffsetsRequest.build_request(plain_map,
          topics: topics,
          api_version: 4,
          current_leader_epoch: 15
        )

      assert result.isolation_level == 0
      assert [%{partitions: [partition]}] = result.topics
      assert partition.current_leader_epoch == 15
    end

    test "plain map with isolation_level and api_version: 5 adds current_leader_epoch" do
      plain_map = %{isolation_level: nil, replica_id: nil, topics: []}

      topics = [{"t", [%{partition_num: 0, timestamp: :earliest}]}]

      result =
        ListOffsetsRequest.build_request(plain_map,
          topics: topics,
          api_version: 5,
          current_leader_epoch: 42
        )

      assert [%{partitions: [partition]}] = result.topics
      assert partition.current_leader_epoch == 42
    end

    test "plain map V2+ respects isolation_level: :read_commited" do
      plain_map = %{isolation_level: nil, replica_id: nil, topics: []}

      topics = [{"t", [%{partition_num: 0, timestamp: :latest}]}]

      result =
        ListOffsetsRequest.build_request(plain_map,
          topics: topics,
          isolation_level: :read_commited
        )

      assert result.isolation_level == 1
    end

    test "plain map V2+ handles multiple topics and partitions" do
      plain_map = %{isolation_level: nil, replica_id: nil, topics: []}

      topics = [
        {"topic-a", [%{partition_num: 0, timestamp: :earliest}, %{partition_num: 1, timestamp: :latest}]},
        {"topic-b", [%{partition_num: 0, timestamp: 123}]}
      ]

      result = ListOffsetsRequest.build_request(plain_map, topics: topics)

      assert length(result.topics) == 2
      [ta, tb] = result.topics
      assert ta.topic == "topic-a"
      assert length(ta.partitions) == 2
      assert tb.topic == "topic-b"
      assert length(tb.partitions) == 1
    end
  end

  describe "Any fallback implementation — minimal empty plain map" do
    test "truly empty plain map triggers V0-V1 fallback path" do
      # The minimal case: a completely empty map. Has no isolation_level,
      # so takes V0-V1 path. Default api_version is 1.
      result =
        ListOffsetsRequest.build_request(%{}, topics: [{"t", [%{partition_num: 0, timestamp: :latest}]}])

      assert result.replica_id == -1
      assert [%{partitions: [partition]}] = result.topics
      refute Map.has_key?(partition, :max_num_offsets)
    end
  end

  defp attach_client_data(request) do
    request
    |> Map.put(:client_id, "test")
    |> Map.put(:correlation_id, 123)
  end
end
