defmodule KafkaEx.Consumer.ConsumerGroup.Assignment do
  @moduledoc """
  The group leader's partition-assignment computation, split out of
  `ConsumerGroup.Manager`: fetch the assignable partitions from cluster metadata
  (retrying `UNKNOWN_TOPIC_OR_PARTITION`), run the assignment strategy over the
  members, and translate to/from the SyncGroup wire shape.

  Run only by the group leader, kept separate from the coordinator FSM.
  """

  alias KafkaEx.API, as: KafkaExAPI
  require Logger

  @max_topic_retries 5
  @topic_retry_base_delay_ms 100
  @topic_retry_max_delay_ms 5000

  @doc """
  Topic/partition tuples the leader can assign, from cluster metadata. Retries
  `UNKNOWN_TOPIC_OR_PARTITION` with exponential backoff (Java-client pattern);
  after `#{@max_topic_retries}` attempts it assigns whatever topics were found.
  """
  @spec assignable_partitions(pid(), [binary()], binary()) :: [{binary(), integer()}]
  def assignable_partitions(client, topics, group_name),
    do: assignable_partitions(client, topics, group_name, 1)

  defp assignable_partitions(client, topics, group_name, attempt) do
    {:ok, cluster_metadata} = KafkaExAPI.metadata(client)
    {found_partitions, missing_topics} = partition_topics_by_availability(cluster_metadata, topics)
    handle_topic_availability(client, topics, group_name, found_partitions, missing_topics, attempt)
  end

  defp partition_topics_by_availability(cluster_metadata, topics) do
    Enum.reduce(topics, {[], []}, fn topic, {found_acc, missing_acc} ->
      case get_partitions_for_topic(cluster_metadata, topic) do
        [] -> {found_acc, [topic | missing_acc]}
        partitions -> {found_acc ++ Enum.map(partitions, &{topic, &1}), missing_acc}
      end
    end)
  end

  defp handle_topic_availability(_client, _topics, _group_name, found_partitions, [], _attempt),
    do: found_partitions

  defp handle_topic_availability(_client, _topics, group_name, found_partitions, missing_topics, attempt)
       when attempt >= @max_topic_retries do
    Logger.warning(
      "Consumer group #{group_name} could not find topics #{Enum.join(missing_topics, ", ")} " <>
        "after #{@max_topic_retries} attempts (UNKNOWN_TOPIC_OR_PARTITION)"
    )

    found_partitions
  end

  defp handle_topic_availability(client, topics, group_name, _found_partitions, missing_topics, attempt) do
    sleep_time = calculate_topic_backoff(attempt)

    Logger.info(
      "Consumer group #{group_name}: topics #{Enum.join(missing_topics, ", ")} not found. " <>
        "Retrying in #{sleep_time}ms (attempt #{attempt}/#{@max_topic_retries})"
    )

    :timer.sleep(sleep_time)
    assignable_partitions(client, topics, group_name, attempt + 1)
  end

  defp calculate_topic_backoff(attempt) do
    delay = @topic_retry_base_delay_ms * round(:math.pow(2, attempt - 1))
    min(delay, @topic_retry_max_delay_ms)
  end

  defp get_partitions_for_topic(cluster_metadata, topic_name) do
    case Map.get(cluster_metadata.topics, topic_name) do
      nil -> []
      topic -> Enum.map(topic.partitions, & &1.partition_id)
    end
  end

  @doc """
  Run the assignment strategy `callback` over `members`/`partitions` (the leader's
  job) and pack each member's result into the SyncGroup wire shape
  `[{member_id, [{topic, [partition]}]}]`.
  """
  @spec for_members([binary()], [{binary(), integer()}], function()) :: [{binary(), list()}]
  def for_members(members, partitions, callback) do
    assignments = Map.new(callback.(members, partitions))
    Enum.map(members, &{&1, pack_assignments(Map.get(assignments, &1, []))})
  end

  defp pack_assignments(assignments) do
    assignments
    |> Enum.reduce(%{}, fn {topic, partition}, acc ->
      Map.update(acc, topic, [partition], &(&1 ++ [partition]))
    end)
    |> Map.to_list()
  end

  @doc "Translate leader assignments to the protocol-agnostic SyncGroup request shape."
  @spec format_for_sync_group([{binary(), list()}]) :: [map()]
  def format_for_sync_group(assignments) do
    Enum.map(assignments, fn {member_id, topic_partitions} ->
      %{member_id: member_id, topic_partitions: topic_partitions}
    end)
  end

  @doc "Flatten a SyncGroup response's partition_assignments into `[{topic, partition}]`."
  @spec from_sync_response([%{topic: binary(), partitions: [integer()]}]) :: [{binary(), integer()}]
  def from_sync_response(partition_assignments) do
    Enum.flat_map(partition_assignments, fn assignment ->
      Enum.map(assignment.partitions, &{assignment.topic, &1})
    end)
  end
end
