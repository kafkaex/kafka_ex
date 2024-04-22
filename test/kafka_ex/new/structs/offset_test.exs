defmodule KafkaEx.New.Structs.OffsetTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Structs.Offset

  describe "from_list_offset/2" do
    test "creates offset with v0 partition responses" do
      result = Offset.from_list_offset("test-topic", [%{offset: 1, partition: 2}])

      assert result == %Offset{
               topic: "test-topic",
               partition_offsets: [
                 %Offset.PartitionOffset{offset: 1, partition: 2, timestamp: -1}
               ]
             }
    end

    test "creates offset with v1 partition responses" do
      result = Offset.from_list_offset("test-topic", [%{offset: 1, partition: 2}])

      assert result == %Offset{
               topic: "test-topic",
               partition_offsets: [
                 %Offset.PartitionOffset{offset: 1, partition: 2, timestamp: -1}
               ]
             }
    end

    test "creates offset with v2 partition responses" do
      result = Offset.from_list_offset("test-topic", [%{offset: 1, partition: 2, timestamp: 3}])

      assert result == %Offset{
               topic: "test-topic",
               partition_offsets: [
                 %Offset.PartitionOffset{offset: 1, partition: 2, timestamp: 3}
               ]
             }
    end
  end
end
