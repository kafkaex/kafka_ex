defmodule KafkaEx.New.Structs.Offset.PartitionOffsetTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Structs.Offset.PartitionOffset

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
               timestamp: nil
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
               timestamp: nil
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
               timestamp: nil
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

  describe "build/1 - All fields present" do
    test "returns struct with all fields when provided" do
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
               metadata: "test-metadata"
             }
    end
  end
end
