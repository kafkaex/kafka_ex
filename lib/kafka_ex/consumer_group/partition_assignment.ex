defmodule KafkaEx.ConsumerGroup.PartitionAssignment do
  @moduledoc """
  Contains useful partition assignment algorithms for consumer groups
  """

  alias KafkaEx.GenConsumer

  @doc """
  Round robin assignment

  Iterates over the partitions and members, giving the first member the first
  partition, the second member the second partition, etc, looping back to the
  beginning of the list of members when finished.

  Example:
      iex> KafkaEx.ConsumerGroup.PartitionAssignment(["m1", "m2"], [{"t1", 0}, {"t2, 1"}, {"t3", 2}])
      %{"m1" => [{"t1", 0}, {"t3", 2}], "m2" => [{"t2", 1}]}
  """
  @spec round_robin([binary], [GenConsumer.partition]) ::
    %{binary => [GenConsumer.partition]}
  def round_robin(members, partitions) do
    members
    |> Stream.cycle
    |> Enum.zip(partitions)
    |> Enum.reduce(%{}, fn({member, partition}, assignments) ->
      Map.update(assignments, member, [partition], &(&1 ++ [partition]))
    end)
  end
end
