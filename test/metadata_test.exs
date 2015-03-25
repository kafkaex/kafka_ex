defmodule KafkaEx.Metadata.Test do
  use ExUnit.Case

  test "set_update_threshold with a integer value sets the update threshold" do
    metadata = KafkaEx.Metadata.new([])
    value = 10 * 60 * 60 * 1000
    metadata = KafkaEx.Metadata.set_update_threshold(metadata, value)
    assert metadata.update_threshold == value
  end

  test "set_update_threshold with a non-integer value raises an exception" do
    metadata = KafkaEx.Metadata.new([])
    assert_raise RuntimeError, "Update threshold value must be an integer number of milliseconds", fn ->
      KafkaEx.Metadata.set_update_threshold(metadata, "100")
    end
  end
end
