defmodule KafkaEx.New.Structs.PartitionTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Structs.Partition

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
      partition = Partition.from_partition_metadata(metadata)

      assert partition.partition_id == 1
    end

    test "set partition leader", %{metadata: metadata} do
      partition = Partition.from_partition_metadata(metadata)

      assert partition.leader == 2
    end

    test "set partition replicas", %{metadata: metadata} do
      partition = Partition.from_partition_metadata(metadata)

      assert partition.replicas == [123]
    end

    test "set partition isr", %{metadata: metadata} do
      partition = Partition.from_partition_metadata(metadata)

      assert partition.isr == [21]
    end
  end
end
