defmodule KafkaEx.Messages.OffsetAndMetadataTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Messages.OffsetAndMetadata

  describe "new/1" do
    test "creates with offset only" do
      result = OffsetAndMetadata.new(100)
      assert %OffsetAndMetadata{offset: 100, metadata: "", leader_epoch: nil} = result
    end

    test "creates with offset 0" do
      result = OffsetAndMetadata.new(0)
      assert result.offset == 0
      assert result.metadata == ""
    end

    test "creates with large offset" do
      large_offset = 9_223_372_036_854_775_807
      result = OffsetAndMetadata.new(large_offset)
      assert result.offset == large_offset
    end

    test "raises FunctionClauseError for negative offset" do
      assert_raise FunctionClauseError, fn ->
        OffsetAndMetadata.new(-1)
      end
    end

    test "raises FunctionClauseError for non-integer offset" do
      assert_raise FunctionClauseError, fn ->
        OffsetAndMetadata.new("100")
      end
    end
  end

  describe "new/2" do
    test "creates with offset and metadata" do
      result = OffsetAndMetadata.new(100, "consumer-v1")
      assert %OffsetAndMetadata{offset: 100, metadata: "consumer-v1"} = result
    end

    test "creates with empty metadata" do
      result = OffsetAndMetadata.new(100, "")
      assert result.metadata == ""
    end

    test "creates with long metadata" do
      metadata = String.duplicate("x", 1000)
      result = OffsetAndMetadata.new(100, metadata)
      assert result.metadata == metadata
    end

    test "raises FunctionClauseError for non-string metadata" do
      assert_raise FunctionClauseError, fn ->
        OffsetAndMetadata.new(100, :metadata)
      end
    end
  end

  describe "build/1" do
    test "builds with required offset only" do
      result = OffsetAndMetadata.build(offset: 500)
      assert %OffsetAndMetadata{offset: 500, metadata: "", leader_epoch: nil} = result
    end

    test "builds with offset and metadata" do
      result = OffsetAndMetadata.build(offset: 500, metadata: "v2")
      assert result.offset == 500
      assert result.metadata == "v2"
    end

    test "builds with all fields" do
      result = OffsetAndMetadata.build(offset: 100, metadata: "meta", leader_epoch: 5)
      assert result.offset == 100
      assert result.metadata == "meta"
      assert result.leader_epoch == 5
    end

    test "uses empty string as default metadata" do
      result = OffsetAndMetadata.build(offset: 100)
      assert result.metadata == ""
    end

    test "uses nil as default leader_epoch" do
      result = OffsetAndMetadata.build(offset: 100)
      assert result.leader_epoch == nil
    end

    test "raises KeyError when offset is missing" do
      assert_raise KeyError, ~r/key :offset not found/, fn ->
        OffsetAndMetadata.build(metadata: "test")
      end
    end

    test "ignores extra options" do
      result = OffsetAndMetadata.build(offset: 100, extra: "ignored")
      assert result.offset == 100
      refute Map.has_key?(result, :extra)
    end
  end

  describe "offset/1" do
    test "returns offset value" do
      om = OffsetAndMetadata.new(12345)
      assert OffsetAndMetadata.offset(om) == 12345
    end

    test "returns zero offset" do
      om = OffsetAndMetadata.new(0)
      assert OffsetAndMetadata.offset(om) == 0
    end
  end

  describe "metadata/1" do
    test "returns metadata string" do
      om = OffsetAndMetadata.new(100, "my-metadata")
      assert OffsetAndMetadata.metadata(om) == "my-metadata"
    end

    test "returns empty metadata" do
      om = OffsetAndMetadata.new(100)
      assert OffsetAndMetadata.metadata(om) == ""
    end
  end

  describe "leader_epoch/1" do
    test "returns leader epoch when set" do
      om = OffsetAndMetadata.build(offset: 100, leader_epoch: 7)
      assert OffsetAndMetadata.leader_epoch(om) == 7
    end

    test "returns nil when leader_epoch not set" do
      om = OffsetAndMetadata.new(100)
      assert OffsetAndMetadata.leader_epoch(om) == nil
    end

    test "returns zero leader epoch" do
      om = OffsetAndMetadata.build(offset: 100, leader_epoch: 0)
      assert OffsetAndMetadata.leader_epoch(om) == 0
    end
  end

  describe "struct behavior" do
    test "can pattern match on struct" do
      om = OffsetAndMetadata.new(100, "test")
      assert %OffsetAndMetadata{offset: 100, metadata: "test"} = om
    end

    test "can access fields directly" do
      om = OffsetAndMetadata.build(offset: 200, metadata: "meta", leader_epoch: 3)
      assert om.offset == 200
      assert om.metadata == "meta"
      assert om.leader_epoch == 3
    end

    test "default values in struct" do
      om = %OffsetAndMetadata{offset: 100}
      assert om.metadata == ""
      assert om.leader_epoch == nil
    end

    test "can update struct" do
      om = OffsetAndMetadata.new(100)
      updated = %{om | metadata: "updated"}
      assert updated.metadata == "updated"
      assert updated.offset == 100
    end
  end

  describe "use cases" do
    test "represents committed offset for consumer group" do
      # Typical usage: consumer commits offset with metadata
      om =
        OffsetAndMetadata.build(
          offset: 12345,
          metadata: "consumer-instance-1"
        )

      assert om.offset == 12345
      assert om.metadata == "consumer-instance-1"
    end

    test "represents offset with leader epoch for fencing" do
      # V3+ usage with leader epoch
      om =
        OffsetAndMetadata.build(
          offset: 999,
          metadata: "",
          leader_epoch: 42
        )

      assert om.offset == 999
      assert om.leader_epoch == 42
    end
  end
end
