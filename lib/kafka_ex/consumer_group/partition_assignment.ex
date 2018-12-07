defmodule KafkaEx.ConsumerGroup.PartitionAssignment do
  @moduledoc """
  Contains typespecs and reference algorithms for assigning partitions

  `round_robin/2` is used by `KafkaEx.ConsumerGroup` by default and should
  suffice in most cases.

  For custom assignments, any function matching the
  `t:callback/0` type spec can be used.
  """

  @typedoc """
  The ID (string) of a member of a consumer group, assigned by a Kafka broker.
  """
  @type member_id :: binary

  @typedoc """
  The string name of a Kafka topic.
  """
  @type topic :: binary

  @typedoc """
  The integer ID of a partition of a Kafka topic.
  """
  @type partition_id :: integer

  @typedoc """
  A partition of a single topic (embeds the name of the topic).
  """
  @type partition :: {topic, partition_id}

  @typedoc """
  A function that can assign partitions.

  `members` is a list of member IDs and `partitions` is a list of partitions
  that need to be assigned to a group member.

  The return value must be a map with member IDs as keys and a list of
  partition assignments as values. For each member ID in the returned map, the
  assigned partitions will become the `assignments` argument to
  `KafkaEx.GenConsumer.Supervisor.start_link/4` in the corresponding member
  process. Any member that's omitted from the return value will not be assigned
  any partitions.

  ### Example

  Given the following `members` and `partitions` to be assigned:

  ```
  members = ["member1", "member2", "member3"]
  partitions = [{"topic", 0}, {"topic", 1}, {"topic", 2}]
  ```

  One possible assignment is as follows:

  ```
  ExampleGenConsumer.assign_partitions(members, partitions)
  #=> %{"member1" => [{"topic", 0}, {"topic", 2}], "member2" => [{"topic", 1}]}
  ```

  In this case, the consumer group process for `"member1"` will launch two
  `KafkaEx.GenConsumer` processes (one for each of its assigned partitions),
  `"member2"` will launch one `KafkaEx.GenConsumer` process, and `"member3"` will
  launch no processes.
  """
  @type callback ::
          (members :: [member_id], partitions :: [partition] ->
             %{member_id => [partition]})

  @doc """
  Round robin assignment

  Iterates over the partitions and members, giving the first member the first
  partition, the second member the second partition, etc, looping back to the
  beginning of the list of members when finished.

  Example:
      iex> KafkaEx.ConsumerGroup.PartitionAssignment(["m1", "m2"], [{"t1", 0}, {"t2, 1"}, {"t3", 2}])
      %{"m1" => [{"t1", 0}, {"t3", 2}], "m2" => [{"t2", 1}]}
  """
  @spec round_robin([binary], [partition]) :: %{binary => [partition]}
  def round_robin(members, partitions) do
    members
    |> Stream.cycle()
    |> Enum.zip(partitions)
    |> Enum.reduce(%{}, fn {member, partition}, assignments ->
      Map.update(assignments, member, [partition], &(&1 ++ [partition]))
    end)
  end
end
