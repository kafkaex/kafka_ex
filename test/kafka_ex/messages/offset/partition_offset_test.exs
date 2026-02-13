defmodule KafkaEx.Messages.Offset.PartitionOffsetTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Messages.Offset.PartitionOffset

  describe "build/1 - ListOffsets API" do
    test "returns struct with timestamp (most common case)" do
      result = PartitionOffset.build(%{partition: 1, offset: 2, error_code: :no_error, timestamp: 123})

      assert result == %PartitionOffset{
               partition: 1,
               error_code: :no_error,
               offset: 2,
               timestamp: 123,
               metadata: nil
             }
    end

    test "returns struct with missing timestamp (legacy v0)" do
      result = PartitionOffset.build(%{partition: 1, offset: 2})

      assert result == %PartitionOffset{
               partition: 1,
               error_code: :no_error,
               offset: 2,
               timestamp: -1,
               metadata: nil
             }
    end

    test "returns struct with explicit error_code and no timestamp (legacy v0)" do
      result = PartitionOffset.build(%{partition: 1, offset: 2, error_code: :no_error})

      assert result == %PartitionOffset{
               partition: 1,
               error_code: :no_error,
               offset: 2,
               timestamp: -1,
               metadata: nil
             }
    end
  end

  describe "build/1 - OffsetFetch API" do
    test "returns struct with metadata" do
      result =
        PartitionOffset.build(%{
          partition: 0,
          offset: 42,
          error_code: :no_error,
          metadata: "consumer-1"
        })

      assert result == %PartitionOffset{
               partition: 0,
               offset: 42,
               error_code: :no_error,
               metadata: "consumer-1",
               timestamp: nil,
               leader_epoch: nil
             }
    end

    test "returns struct with empty metadata" do
      result =
        PartitionOffset.build(%{partition: 5, offset: 100, error_code: :no_error, metadata: ""})

      assert result == %PartitionOffset{
               partition: 5,
               offset: 100,
               error_code: :no_error,
               metadata: "",
               timestamp: nil,
               leader_epoch: nil
             }
    end

    test "returns struct with error and metadata" do
      result =
        PartitionOffset.build(%{
          partition: 2,
          offset: -1,
          error_code: :unknown_topic_or_partition,
          metadata: ""
        })

      assert result == %PartitionOffset{
               partition: 2,
               offset: -1,
               error_code: :unknown_topic_or_partition,
               metadata: "",
               timestamp: nil,
               leader_epoch: nil
             }
    end
  end

  describe "build/1 - OffsetCommit API" do
    test "returns struct with only partition and error_code (no offset)" do
      result = PartitionOffset.build(%{partition: 0, error_code: :no_error})

      assert result == %PartitionOffset{
               partition: 0,
               offset: nil,
               error_code: :no_error,
               timestamp: nil,
               metadata: nil
             }
    end

    test "returns struct with error response" do
      result = PartitionOffset.build(%{partition: 3, error_code: :offset_metadata_too_large})

      assert result == %PartitionOffset{
               partition: 3,
               offset: nil,
               error_code: :offset_metadata_too_large,
               timestamp: nil,
               metadata: nil
             }
    end

    test "returns struct with various error codes" do
      error_codes = [
        :no_error,
        :offset_metadata_too_large,
        :group_load_in_progress,
        :not_coordinator_for_group
      ]

      for error_code <- error_codes do
        result = PartitionOffset.build(%{partition: 1, error_code: error_code})

        assert result.partition == 1
        assert result.error_code == error_code
        assert result.offset == nil
        assert result.timestamp == nil
        assert result.metadata == nil
      end
    end
  end

  describe "build/1 - OffsetFetch V5+ (metadata + leader_epoch, no timestamp)" do
    test "returns struct with metadata and leader_epoch, timestamp is nil" do
      result =
        PartitionOffset.build(%{
          partition: 0,
          offset: 500,
          error_code: :no_error,
          metadata: "v5-consumer",
          leader_epoch: 8
        })

      assert result == %PartitionOffset{
               partition: 0,
               offset: 500,
               error_code: :no_error,
               metadata: "v5-consumer",
               timestamp: nil,
               leader_epoch: 8
             }
    end

    test "returns struct with empty metadata and leader_epoch" do
      result =
        PartitionOffset.build(%{
          partition: 3,
          offset: 100,
          error_code: :no_error,
          metadata: "",
          leader_epoch: 2
        })

      assert result == %PartitionOffset{
               partition: 3,
               offset: 100,
               error_code: :no_error,
               metadata: "",
               timestamp: nil,
               leader_epoch: 2
             }
    end

    test "returns struct with leader_epoch -1 (unknown) and metadata" do
      result =
        PartitionOffset.build(%{
          partition: 1,
          offset: 42,
          error_code: :no_error,
          metadata: "meta",
          leader_epoch: -1
        })

      assert result.timestamp == nil
      assert result.metadata == "meta"
      assert result.leader_epoch == -1
    end

    test "returns struct with leader_epoch 0 and metadata" do
      result =
        PartitionOffset.build(%{
          partition: 2,
          offset: 200,
          error_code: :no_error,
          metadata: "",
          leader_epoch: 0
        })

      assert result.timestamp == nil
      assert result.leader_epoch == 0
    end

    test "returns struct with error code and leader_epoch and metadata" do
      result =
        PartitionOffset.build(%{
          partition: 0,
          offset: -1,
          error_code: :unknown_topic_or_partition,
          metadata: "",
          leader_epoch: -1
        })

      assert result == %PartitionOffset{
               partition: 0,
               offset: -1,
               error_code: :unknown_topic_or_partition,
               metadata: "",
               timestamp: nil,
               leader_epoch: -1
             }
    end
  end

  describe "build/1 - ListOffsets V4+ (leader_epoch)" do
    test "returns struct with timestamp and leader_epoch" do
      result =
        PartitionOffset.build(%{
          partition: 0,
          offset: 250,
          error_code: :no_error,
          timestamp: 1_700_000_000_000,
          leader_epoch: 7
        })

      assert result == %PartitionOffset{
               partition: 0,
               offset: 250,
               error_code: :no_error,
               timestamp: 1_700_000_000_000,
               metadata: nil,
               leader_epoch: 7
             }
    end

    test "returns struct with leader_epoch -1 (unknown)" do
      result =
        PartitionOffset.build(%{
          partition: 3,
          offset: 42,
          error_code: :no_error,
          timestamp: 999,
          leader_epoch: -1
        })

      assert result.leader_epoch == -1
    end

    test "returns struct with leader_epoch nil when not provided" do
      result =
        PartitionOffset.build(%{partition: 1, offset: 2, error_code: :no_error, timestamp: 123})

      assert result.leader_epoch == nil
    end
  end

  describe "build/1 - All fields present" do
    test "returns struct with all fields when provided (no leader_epoch)" do
      result =
        PartitionOffset.build(%{
          partition: 7,
          offset: 999,
          error_code: :no_error,
          timestamp: 1_234_567_890,
          metadata: "test-metadata"
        })

      assert result == %PartitionOffset{
               partition: 7,
               offset: 999,
               error_code: :no_error,
               timestamp: 1_234_567_890,
               metadata: "test-metadata",
               leader_epoch: nil
             }
    end

    test "returns struct with all fields including leader_epoch" do
      result =
        PartitionOffset.build(%{
          partition: 7,
          offset: 999,
          error_code: :no_error,
          timestamp: 1_234_567_890,
          metadata: "test-metadata",
          leader_epoch: 5
        })

      assert result == %PartitionOffset{
               partition: 7,
               offset: 999,
               error_code: :no_error,
               timestamp: 1_234_567_890,
               metadata: "test-metadata",
               leader_epoch: 5
             }
    end
  end

  describe "build/1 - pattern match ordering" do
    test "map with all 6 fields matches the first clause (timestamp + metadata + leader_epoch)" do
      # This verifies line 60-61: build(%{partition, offset, error_code, timestamp, metadata, leader_epoch})
      result =
        PartitionOffset.build(%{
          partition: 0,
          offset: 100,
          error_code: :no_error,
          timestamp: 999,
          metadata: "full",
          leader_epoch: 5
        })

      assert result.timestamp == 999
      assert result.metadata == "full"
      assert result.leader_epoch == 5
    end

    test "map with metadata + leader_epoch but no timestamp matches OffsetFetch V5+ clause" do
      # This verifies line 68: build(%{partition, offset, error_code, metadata, leader_epoch})
      # NOT line 69: build(%{partition, offset, error_code, metadata}) which has no leader_epoch
      result =
        PartitionOffset.build(%{
          partition: 0,
          offset: 100,
          error_code: :no_error,
          metadata: "v5-meta",
          leader_epoch: 3
        })

      assert result.timestamp == nil
      assert result.metadata == "v5-meta"
      assert result.leader_epoch == 3
    end

    test "map with metadata but no timestamp and no leader_epoch matches OffsetFetch V0-V4 clause" do
      # This verifies line 69: build(%{partition, offset, error_code, metadata})
      result =
        PartitionOffset.build(%{
          partition: 0,
          offset: 100,
          error_code: :no_error,
          metadata: "v2-meta"
        })

      assert result.timestamp == nil
      assert result.metadata == "v2-meta"
      assert result.leader_epoch == nil
    end

    test "map with timestamp + leader_epoch but no metadata matches ListOffsets V4+ clause" do
      # This verifies line 63-64: build(%{partition, offset, error_code, timestamp, leader_epoch})
      result =
        PartitionOffset.build(%{
          partition: 0,
          offset: 100,
          error_code: :no_error,
          timestamp: 888,
          leader_epoch: 7
        })

      assert result.timestamp == 888
      assert result.metadata == nil
      assert result.leader_epoch == 7
    end

    test "map with timestamp + metadata but no leader_epoch matches ListOffsets V1-V3 clause" do
      # This verifies line 66: build(%{partition, offset, error_code, timestamp, metadata})
      result =
        PartitionOffset.build(%{
          partition: 0,
          offset: 100,
          error_code: :no_error,
          timestamp: 777,
          metadata: "some"
        })

      assert result.timestamp == 777
      assert result.metadata == "some"
      assert result.leader_epoch == nil
    end
  end

  describe "to_offset_and_metadata/1" do
    alias KafkaEx.Messages.OffsetAndMetadata

    test "converts to OffsetAndMetadata with leader_epoch" do
      po =
        PartitionOffset.build(%{
          partition: 0,
          offset: 100,
          error_code: :no_error,
          timestamp: 500,
          leader_epoch: 3
        })

      result = PartitionOffset.to_offset_and_metadata(po)

      assert result == %OffsetAndMetadata{offset: 100, metadata: "", leader_epoch: 3}
    end

    test "converts to OffsetAndMetadata from OffsetFetch V5+ build (metadata + leader_epoch, no timestamp)" do
      po =
        PartitionOffset.build(%{
          partition: 0,
          offset: 500,
          error_code: :no_error,
          metadata: "v5-consumer",
          leader_epoch: 8
        })

      result = PartitionOffset.to_offset_and_metadata(po)

      assert result == %OffsetAndMetadata{offset: 500, metadata: "v5-consumer", leader_epoch: 8}
    end

    test "converts to OffsetAndMetadata with nil leader_epoch" do
      po =
        PartitionOffset.build(%{
          partition: 0,
          offset: 100,
          error_code: :no_error,
          metadata: "v1"
        })

      result = PartitionOffset.to_offset_and_metadata(po)

      assert result == %OffsetAndMetadata{offset: 100, metadata: "v1", leader_epoch: nil}
    end

    test "returns nil when offset is nil" do
      po = PartitionOffset.build(%{partition: 0, error_code: :no_error})

      assert PartitionOffset.to_offset_and_metadata(po) == nil
    end
  end
end
