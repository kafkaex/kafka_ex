defmodule KafkaEx.New.Kafka.PartitionInfoTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Kafka.PartitionInfo

  describe "from_partition_metadata/1" do
    setup do
      metadata = %{
        error_code: 0,
        partition: 1,
        leader: 2,
        replicas: [123],
        isr: [21]
      }

      {:ok, %{metadata: metadata}}
    end

    test "set partition id", %{metadata: metadata} do
      partition = PartitionInfo.from_partition_metadata(metadata)

      assert partition.partition_id == 1
    end

    test "set partition leader", %{metadata: metadata} do
      partition = PartitionInfo.from_partition_metadata(metadata)

      assert partition.leader == 2
    end

    test "set partition replicas", %{metadata: metadata} do
      partition = PartitionInfo.from_partition_metadata(metadata)

      assert partition.replicas == [123]
    end

    test "set partition isr", %{metadata: metadata} do
      partition = PartitionInfo.from_partition_metadata(metadata)

      assert partition.isr == [21]
    end
  end

  describe "build/1" do
    test "builds with required partition_id" do
      result = PartitionInfo.build(partition_id: 0)
      assert result.partition_id == 0
      assert result.leader == -1
      assert result.replicas == []
      assert result.isr == []
    end

    test "builds with all fields" do
      result = PartitionInfo.build(
        partition_id: 5,
        leader: 2,
        replicas: [1, 2, 3],
        isr: [1, 2]
      )

      assert result.partition_id == 5
      assert result.leader == 2
      assert result.replicas == [1, 2, 3]
      assert result.isr == [1, 2]
    end

    test "raises KeyError when partition_id is missing" do
      assert_raise KeyError, ~r/key :partition_id not found/, fn ->
        PartitionInfo.build(leader: 1)
      end
    end
  end

  describe "partition/1" do
    test "returns partition id" do
      info = PartitionInfo.build(partition_id: 7)
      assert PartitionInfo.partition(info) == 7
    end
  end

  describe "leader/1" do
    test "returns leader node id" do
      info = PartitionInfo.build(partition_id: 0, leader: 3)
      assert PartitionInfo.leader(info) == 3
    end

    test "returns -1 when no leader" do
      info = PartitionInfo.build(partition_id: 0)
      assert PartitionInfo.leader(info) == -1
    end
  end

  describe "replicas/1" do
    test "returns replica list" do
      info = PartitionInfo.build(partition_id: 0, replicas: [1, 2, 3])
      assert PartitionInfo.replicas(info) == [1, 2, 3]
    end

    test "returns empty list when no replicas" do
      info = PartitionInfo.build(partition_id: 0)
      assert PartitionInfo.replicas(info) == []
    end
  end

  describe "in_sync_replicas/1" do
    test "returns ISR list" do
      info = PartitionInfo.build(partition_id: 0, isr: [1, 2])
      assert PartitionInfo.in_sync_replicas(info) == [1, 2]
    end

    test "returns empty list when no ISR" do
      info = PartitionInfo.build(partition_id: 0)
      assert PartitionInfo.in_sync_replicas(info) == []
    end
  end

  describe "struct behavior" do
    test "can pattern match on struct" do
      info = PartitionInfo.build(partition_id: 0, leader: 1)
      assert %PartitionInfo{partition_id: 0, leader: 1} = info
    end

    test "can access fields directly" do
      info = PartitionInfo.build(partition_id: 3, leader: 2, replicas: [1, 2], isr: [1])
      assert info.partition_id == 3
      assert info.leader == 2
      assert info.replicas == [1, 2]
      assert info.isr == [1]
    end

    test "default values" do
      info = %PartitionInfo{}
      assert info.partition_id == nil
      assert info.leader == -1
      assert info.replicas == []
      assert info.isr == []
    end
  end
end
