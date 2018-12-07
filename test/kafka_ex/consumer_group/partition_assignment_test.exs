defmodule KafkaEx.ConsumerGroup.PartitionAssignmentTest do
  use ExUnit.Case

  alias KafkaEx.ConsumerGroup.PartitionAssignment

  test "round robin partition assignment works" do
    topic = "topic"
    members = ["member1", "member2"]
    partitions = [{topic, 0}, {topic, 1}, {topic, 2}]

    assignments = PartitionAssignment.round_robin(members, partitions)

    expected = %{
      "member1" => [{topic, 0}, {topic, 2}],
      "member2" => [{topic, 1}]
    }

    assert expected == assignments
  end
end
