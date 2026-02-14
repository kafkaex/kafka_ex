defmodule KafkaEx.Protocol.Kayrock.Fetch.RequestHelpersTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Protocol.Kayrock.Fetch.RequestHelpers

  describe "extract_common_fields/1" do
    test "extracts required fields" do
      opts = [
        topic: "test_topic",
        partition: 0,
        offset: 100
      ]

      fields = RequestHelpers.extract_common_fields(opts)

      assert fields.topic == "test_topic"
      assert fields.partition == 0
      assert fields.offset == 100
    end

    test "uses defaults for optional fields" do
      opts = [
        topic: "test_topic",
        partition: 0,
        offset: 0
      ]

      fields = RequestHelpers.extract_common_fields(opts)

      assert fields.max_bytes == 1_000_000
      assert fields.max_wait_time == 10_000
      assert fields.min_bytes == 1
    end

    test "allows overriding optional fields" do
      opts = [
        topic: "test_topic",
        partition: 0,
        offset: 0,
        max_bytes: 500_000,
        max_wait_time: 5_000,
        min_bytes: 100
      ]

      fields = RequestHelpers.extract_common_fields(opts)

      assert fields.max_bytes == 500_000
      assert fields.max_wait_time == 5_000
      assert fields.min_bytes == 100
    end
  end

  describe "build_topics/2" do
    test "builds topics structure for V0-V4" do
      fields = %{
        topic: "test_topic",
        partition: 0,
        offset: 100,
        max_bytes: 1_000_000
      }

      topics = RequestHelpers.build_topics(fields, api_version: 3)

      assert [%{topic: "test_topic", partitions: [partition]}] = topics
      assert partition.partition == 0
      assert partition.fetch_offset == 100
      assert partition.partition_max_bytes == 1_000_000
      refute Map.has_key?(partition, :log_start_offset)
    end

    test "builds topics structure for V5+ with log_start_offset" do
      fields = %{
        topic: "test_topic",
        partition: 0,
        offset: 100,
        max_bytes: 1_000_000
      }

      topics = RequestHelpers.build_topics(fields, api_version: 5)

      assert [%{topic: "test_topic", partitions: [partition]}] = topics
      assert partition.partition == 0
      assert partition.fetch_offset == 100
      assert partition.partition_max_bytes == 1_000_000
      assert partition.log_start_offset == 0
    end

    test "allows custom log_start_offset for V5+" do
      fields = %{
        topic: "test_topic",
        partition: 0,
        offset: 100,
        max_bytes: 1_000_000
      }

      topics = RequestHelpers.build_topics(fields, api_version: 5, log_start_offset: 50)

      assert [%{partitions: [partition]}] = topics
      assert partition.log_start_offset == 50
    end
  end

  describe "populate_request/3" do
    test "populates request struct with common fields" do
      request = %{replica_id: nil, max_wait_time: nil, min_bytes: nil, topics: nil}

      fields = %{
        max_wait_time: 10_000,
        min_bytes: 1
      }

      topics = [%{topic: "test"}]

      result = RequestHelpers.populate_request(request, fields, topics)

      assert result.replica_id == -1
      assert result.max_wait_time == 10_000
      assert result.min_bytes == 1
      assert result.topics == topics
    end
  end

  describe "add_max_bytes/3" do
    test "adds max_bytes for V3+" do
      request = %{max_bytes: nil}
      fields = %{max_bytes: 1_000_000}

      result = RequestHelpers.add_max_bytes(request, fields, 3)

      assert result.max_bytes == 1_000_000
    end

    test "does not modify request for V0-V2" do
      request = %{max_bytes: nil}
      fields = %{max_bytes: 1_000_000}

      result = RequestHelpers.add_max_bytes(request, fields, 2)

      assert result.max_bytes == nil
    end
  end

  describe "add_isolation_level/3" do
    test "adds isolation_level for V4+" do
      request = %{isolation_level: nil}

      result = RequestHelpers.add_isolation_level(request, [], 4)

      assert result.isolation_level == 0
    end

    test "allows custom isolation_level" do
      request = %{isolation_level: nil}

      result = RequestHelpers.add_isolation_level(request, [isolation_level: 1], 4)

      assert result.isolation_level == 1
    end

    test "does not modify request for V0-V3" do
      request = %{isolation_level: nil}

      result = RequestHelpers.add_isolation_level(request, [isolation_level: 1], 3)

      assert result.isolation_level == nil
    end
  end

  describe "add_session_fields/3" do
    test "adds session fields for V7+" do
      request = %{session_id: nil, session_epoch: nil, forgotten_topics_data: nil}

      result = RequestHelpers.add_session_fields(request, [], 7)

      assert result.session_id == 0
      assert result.session_epoch == -1
      assert result.forgotten_topics_data == []
    end

    test "allows custom session fields" do
      request = %{session_id: nil, session_epoch: nil, forgotten_topics_data: nil}

      opts = [
        session_id: 123,
        epoch: 5,
        forgotten_topics_data: [%{topic: "old_topic"}]
      ]

      result = RequestHelpers.add_session_fields(request, opts, 7)

      assert result.session_id == 123
      assert result.session_epoch == 5
      assert result.forgotten_topics_data == [%{topic: "old_topic"}]
    end

    test "does not modify request for V0-V6" do
      request = %{session_id: nil, session_epoch: nil, forgotten_topics_data: nil}

      result = RequestHelpers.add_session_fields(request, [session_id: 123], 6)

      assert result.session_id == nil
    end
  end

  describe "add_current_leader_epoch/3" do
    test "adds current_leader_epoch to partitions for V9+" do
      request = %{
        topics: [
          %{
            topic: "test_topic",
            partitions: [
              %{partition: 0, fetch_offset: 100, partition_max_bytes: 1_000_000}
            ]
          }
        ]
      }

      result = RequestHelpers.add_current_leader_epoch(request, [], 9)

      [%{partitions: [partition]}] = result.topics
      assert partition.current_leader_epoch == -1
    end

    test "allows custom current_leader_epoch" do
      request = %{
        topics: [
          %{
            topic: "test_topic",
            partitions: [
              %{partition: 0, fetch_offset: 100, partition_max_bytes: 1_000_000}
            ]
          }
        ]
      }

      result = RequestHelpers.add_current_leader_epoch(request, [current_leader_epoch: 5], 9)

      [%{partitions: [partition]}] = result.topics
      assert partition.current_leader_epoch == 5
    end

    test "does not modify request for V0-V8" do
      request = %{
        topics: [
          %{
            topic: "test_topic",
            partitions: [
              %{partition: 0, fetch_offset: 100, partition_max_bytes: 1_000_000}
            ]
          }
        ]
      }

      result = RequestHelpers.add_current_leader_epoch(request, [current_leader_epoch: 5], 8)

      [%{partitions: [partition]}] = result.topics
      refute Map.has_key?(partition, :current_leader_epoch)
    end

    test "adds current_leader_epoch to multiple partitions" do
      request = %{
        topics: [
          %{
            topic: "topic1",
            partitions: [
              %{partition: 0, fetch_offset: 0},
              %{partition: 1, fetch_offset: 100}
            ]
          }
        ]
      }

      result = RequestHelpers.add_current_leader_epoch(request, [current_leader_epoch: 3], 10)

      [%{partitions: partitions}] = result.topics
      assert Enum.all?(partitions, fn p -> p.current_leader_epoch == 3 end)
    end
  end

  describe "add_rack_id/3" do
    test "adds rack_id for V11+" do
      request = %{rack_id: nil}

      result = RequestHelpers.add_rack_id(request, [], 11)

      assert result.rack_id == ""
    end

    test "allows custom rack_id" do
      request = %{rack_id: nil}

      result = RequestHelpers.add_rack_id(request, [rack_id: "us-east-1a"], 11)

      assert result.rack_id == "us-east-1a"
    end

    test "does not modify request for V0-V10" do
      request = %{rack_id: nil}

      result = RequestHelpers.add_rack_id(request, [rack_id: "us-east-1a"], 10)

      assert result.rack_id == nil
    end
  end

  describe "build_request_v7_plus/3" do
    test "builds complete V7 request" do
      request = %Kayrock.Fetch.V7.Request{}

      opts = [
        topic: "test-topic",
        partition: 0,
        offset: 100,
        session_id: 42,
        epoch: 3
      ]

      result = RequestHelpers.build_request_v7_plus(request, opts, 7)

      assert result.replica_id == -1
      assert result.max_bytes == 1_000_000
      assert result.isolation_level == 0
      assert result.session_id == 42
      assert result.session_epoch == 3
      assert [%{topic: "test-topic", partitions: [p]}] = result.topics
      assert p.log_start_offset == 0
      refute Map.has_key?(p, :current_leader_epoch)
    end

    test "builds complete V9 request with current_leader_epoch" do
      request = %Kayrock.Fetch.V9.Request{}

      opts = [
        topic: "test-topic",
        partition: 0,
        offset: 100,
        current_leader_epoch: 7
      ]

      result = RequestHelpers.build_request_v7_plus(request, opts, 9)

      assert result.session_id == 0
      assert result.session_epoch == -1
      assert [%{partitions: [p]}] = result.topics
      assert p.current_leader_epoch == 7
    end

    test "builds complete V11 request with rack_id" do
      request = %Kayrock.Fetch.V11.Request{}

      opts = [
        topic: "test-topic",
        partition: 0,
        offset: 100,
        rack_id: "rack-1"
      ]

      result = RequestHelpers.build_request_v7_plus(request, opts, 11)

      assert result.rack_id == "rack-1"
      assert [%{partitions: [p]}] = result.topics
      assert p.current_leader_epoch == -1
    end
  end
end
