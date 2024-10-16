defmodule KafkaEx.New.Structs.Offset.PartitionOffsetTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Structs.Offset.PartitionOffset

  describe "build/1" do
    test "returns struct with missing timestamp" do
      result = PartitionOffset.build(%{partition: 1, offset: 2})

      assert result == %PartitionOffset{
               partition: 1,
               error_code: :no_error,
               offset: 2,
               timestamp: -1
             }
    end

    test "returns struct with timestamp" do
      result = PartitionOffset.build(%{partition: 1, offset: 2, error_code: :no_error, timestamp: 123})

      assert result == %PartitionOffset{
               partition: 1,
               error_code: :no_error,
               offset: 2,
               timestamp: 123
             }
    end
  end
end
