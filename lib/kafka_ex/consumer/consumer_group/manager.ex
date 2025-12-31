defmodule KafkaEx.Consumer.ConsumerGroup.Manager do
  @moduledoc false

  # actual consumer group management implementation

  use GenServer

  alias KafkaEx.Consumer.ConsumerGroup
  alias KafkaEx.Consumer.ConsumerGroup.Heartbeat
  alias KafkaEx.Consumer.ConsumerGroup.PartitionAssignment
  alias KafkaEx.API, as: KafkaExAPI
  require Logger

  defmodule State do
    @moduledoc false
    defstruct [
      :supervisor_pid,
      :client,
      :heartbeat_interval,
      :session_timeout,
      :session_timeout_padding,
      :gen_consumer_module,
      :consumer_module,
      :consumer_opts,
      :partition_assignment_callback,
      :group_name,
      :topics,
      :member_id,
      :leader_id,
      :consumer_supervisor_pid,
      :members,
      :generation_id,
      :assignments,
      :heartbeat_timer
    ]

    @type t :: %__MODULE__{}
  end

  @heartbeat_interval 5_000
  @session_timeout 30_000
  @session_timeout_padding 10_000
  @max_join_retries 6

  @type assignments :: [{binary(), integer()}]

  # Client API

  @doc false
  # use `KafkaEx.Consumer.ConsumerGroup.start_link/4` instead
  @spec start_link({
          {module, module},
          binary,
          [binary],
          KafkaEx.GenConsumer.options()
        }) :: GenServer.on_start()
  def start_link({
        {gen_consumer_module, consumer_module},
        group_name,
        topics,
        opts
      }) do
    gen_server_opts = Keyword.get(opts, :gen_server_opts, [])
    consumer_opts = Keyword.drop(opts, [:gen_server_opts])

    GenServer.start_link(
      __MODULE__,
      {{gen_consumer_module, consumer_module}, group_name, topics, consumer_opts},
      gen_server_opts
    )
  end

  # GenServer callbacks

  def init({{gen_consumer_module, consumer_module}, group_name, topics, opts}) do
    heartbeat_interval =
      Keyword.get(
        opts,
        :heartbeat_interval,
        Application.get_env(:kafka_ex, :heartbeat_interval, @heartbeat_interval)
      )

    session_timeout =
      Keyword.get(
        opts,
        :session_timeout,
        Application.get_env(:kafka_ex, :session_timeout, @session_timeout)
      )

    session_timeout_padding =
      Keyword.get(
        opts,
        :session_timeout_padding,
        Application.get_env(
          :kafka_ex,
          :session_timeout_padding,
          @session_timeout_padding
        )
      )

    partition_assignment_callback =
      Keyword.get(
        opts,
        :partition_assignment_callback,
        &PartitionAssignment.round_robin/2
      )

    supervisor_pid = Keyword.fetch!(opts, :supervisor_pid)

    consumer_opts =
      Keyword.drop(
        opts,
        [
          :supervisor_pid,
          :heartbeat_interval,
          :session_timeout,
          :partition_assignment_callback
        ]
      )

    client_opts =
      opts
      |> Keyword.take([:uris, :use_ssl, :ssl_options, :auth])
      |> Keyword.put(:consumer_group, group_name)
      |> Keyword.put(:initial_topics, topics)

    {:ok, client} = KafkaEx.Client.start_link(client_opts, :no_name)

    state = %State{
      supervisor_pid: supervisor_pid,
      client: client,
      heartbeat_interval: heartbeat_interval,
      session_timeout: session_timeout,
      session_timeout_padding: session_timeout_padding,
      consumer_module: consumer_module,
      gen_consumer_module: gen_consumer_module,
      partition_assignment_callback: partition_assignment_callback,
      consumer_opts: consumer_opts,
      group_name: group_name,
      topics: topics,
      member_id: nil
    }

    Process.flag(:trap_exit, true)

    {:ok, state, 0}
  end

  ######################################################################
  # handle_call clauses - mostly for ops queries
  def handle_call(:generation_id, _from, state) do
    {:reply, state.generation_id, state}
  end

  def handle_call(:member_id, _from, state) do
    {:reply, state.member_id, state}
  end

  def handle_call(:leader_id, _from, state) do
    {:reply, state.leader_id, state}
  end

  def handle_call(:am_leader, _from, state) do
    {:reply, state.leader_id && state.member_id == state.leader_id, state}
  end

  def handle_call(:assignments, _from, state) do
    {:reply, state.assignments, state}
  end

  def handle_call(:consumer_supervisor_pid, _from, state) do
    {:reply, state.consumer_supervisor_pid, state}
  end

  def handle_call(:group_name, _from, state) do
    {:reply, state.group_name, state}
  end

  ######################################################################

  # If `member_id` and `generation_id` aren't set, we haven't yet joined the
  # group. `member_id` and `generation_id` are initialized by
  # `JoinGroupResponse`.
  def handle_info(
        :timeout,
        %State{generation_id: nil, member_id: nil} = state
      ) do
    {:ok, new_state} = join(state)

    {:noreply, new_state}
  end

  # If the heartbeat gets an error, we need to rebalance.
  def handle_info(
        {:EXIT, heartbeat_timer, {:shutdown, :rebalance}},
        %State{heartbeat_timer: heartbeat_timer} = state
      ) do
    {:ok, state} = rebalance(state)
    {:noreply, state}
  end

  # If the heartbeat gets an unrecoverable error.
  def handle_info(
        {:EXIT, _heartbeat_timer, {:shutdown, {:error, reason}}},
        %State{} = state
      ) do
    {:stop, {:shutdown, {:error, reason}}, state}
  end

  # When terminating, inform the group coordinator that this member is leaving
  # the group so that the group can rebalance without waiting for a session
  # timeout.
  def terminate(_reason, %State{generation_id: nil, member_id: nil} = state) do
    Process.unlink(state.client)
    GenServer.stop(state.client, :normal)
  end

  def terminate(_reason, %State{} = state) do
    {:ok, _state} = leave(state)
    Process.unlink(state.client)
    GenServer.stop(state.client, :normal)

    # should be at end because of race condition (stop heartbeat while it is shutting down)
    # if race condition happens, client will be abandoned
    stop_heartbeat_timer(state)
  end

  ### Helpers

  # `JoinGroupRequest` is used to set the active members of a group. The
  # response blocks until the broker has decided that it has a full list of
  # group members. This requires that all active members send a
  # `JoinGroupRequest`. For active members, this is triggered by the broker
  # responding to a heartbeat with a `:rebalance_in_progress` error code. If
  # any group members fail to send a `JoinGroupRequest` before the session
  # timeout expires, then those group members are removed from the group and
  # synchronization continues without them.
  #
  # `JoinGroupResponse` tells each member its unique member ID as well as the
  # group's current generation ID. The broker will pick one group member to be
  # the leader, which is responsible for assigning partitions to all of the
  # group members. Once a `JoinGroupResponse` is received, all group members
  # must send a `SyncGroupRequest` (see sync/2).
  defp join(state), do: join(state, 1)

  defp join(
         %State{
           client: client,
           session_timeout: session_timeout,
           session_timeout_padding: session_timeout_padding,
           group_name: group_name,
           topics: topics,
           member_id: member_id
         } = state,
         attempt_number
       ) do
    opts = [
      topics: topics,
      session_timeout: session_timeout,
      timeout: session_timeout + session_timeout_padding
    ]

    join_response = KafkaExAPI.join_group(client, group_name, member_id || "", opts)

    # crash the client if we receive an error, but do it with a meaningful
    # error message
    case join_response do
      {:ok, %{error_code: :no_error} = response} ->
        on_successful_join(state, response)

      {:error, :no_broker} ->
        if attempt_number >= @max_join_retries do
          raise "Unable to join consumer group #{state.group_name} after " <>
                  "#{@max_join_retries} attempts"
        end

        Logger.warning(
          "Unable to join consumer group #{inspect(group_name)}.  " <>
            "Will sleep 1 second and try again (attempt number #{attempt_number})"
        )

        :timer.sleep(1000)
        join(state, attempt_number + 1)

      {:ok, %{error_code: error_code}} ->
        raise "Error joining consumer group #{group_name}: " <>
                "#{inspect(error_code)}"

      {:error, reason} ->
        raise "Error joining consumer group #{group_name}: " <>
                "#{inspect(reason)}"
    end
  end

  defp on_successful_join(state, join_response) do
    Logger.debug(fn ->
      "Joined consumer group #{state.group_name} generation " <>
        "#{join_response.generation_id} as #{join_response.member_id}"
    end)

    new_state = %State{
      state
      | leader_id: join_response.leader_id,
        member_id: join_response.member_id,
        generation_id: join_response.generation_id
    }

    # Check if this member is the leader (leader_id matches member_id)
    is_leader = join_response.leader_id == join_response.member_id

    assignments =
      if is_leader do
        # Leader is responsible for assigning partitions to all group members.
        partitions = assignable_partitions(new_state)
        # Extract member IDs from the members list
        member_ids = Enum.map(join_response.members, & &1.member_id)
        assign_partitions(new_state, member_ids, partitions)
      else
        # Follower does not assign partitions; must be empty.
        []
      end

    sync(new_state, assignments)
  end

  # `SyncGroupRequest` is used to distribute partition assignments to all group
  # members. All group members must send this request after receiving a
  # response to a `JoinGroupRequest`. The request blocks until assignments are
  # provided by the leader. The leader sends partition assignments (given by
  # the `assignments` parameter) as part of its `SyncGroupRequest`. For all
  # other members, `assignments` must be empty.
  #
  # `SyncGroupResponse` contains the individual member's partition assignments.
  # Upon receiving a successful `SyncGroupResponse`, a group member is free to
  # start consuming from its assigned partitions, but must send periodic
  # heartbeats to the coordinating broker.
  defp sync(
         %State{
           client: client,
           group_name: group_name,
           member_id: member_id,
           generation_id: generation_id,
           session_timeout: session_timeout,
           session_timeout_padding: session_timeout_padding
         } = state,
         assignments
       ) do
    # Convert assignments to Kayrock format
    group_assignment = format_assignments_for_kayrock(assignments)

    opts = [
      group_assignment: group_assignment,
      timeout: session_timeout + session_timeout_padding
    ]

    {:ok, sync_response} =
      KafkaExAPI.sync_group(client, group_name, generation_id, member_id, opts)

    case sync_response.error_code do
      :no_error ->
        # On a high-latency connection, the join/sync process takes a long
        # time. Send a heartbeat as soon as possible to avoid hitting the
        # session timeout.
        {:ok, state} = start_heartbeat_timer(state)
        {:ok, state} = stop_consumer(state)
        # Convert partition_assignments to the format expected by start_consumer
        consumer_assignments = extract_consumer_assignments(sync_response.partition_assignments)
        start_consumer(state, consumer_assignments)

      :rebalance_in_progress ->
        rebalance(state)
    end
  end

  # Convert assignments from Manager format to Kayrock format for sync_group request
  # Input: [{member_id, [{topic, [partition_ids]}]}]
  # Output: [%{member_id: ..., member_assignment: %Kayrock.MemberAssignment{...}}]
  defp format_assignments_for_kayrock(assignments) do
    Enum.map(assignments, fn {member_id, topic_partitions} ->
      %{
        member_id: member_id,
        member_assignment: %Kayrock.MemberAssignment{
          version: 0,
          partition_assignments:
            Enum.map(topic_partitions, fn {topic, partitions} ->
              %Kayrock.MemberAssignment.PartitionAssignment{
                topic: topic,
                partitions: partitions
              }
            end),
          user_data: ""
        }
      }
    end)
  end

  # Convert partition_assignments from SyncGroup response to consumer format
  # Input: [%PartitionAssignment{topic: ..., partitions: [...]}]
  # Output: [{topic, partition}]
  defp extract_consumer_assignments(partition_assignments) do
    Enum.flat_map(partition_assignments, fn assignment ->
      Enum.map(assignment.partitions, fn partition ->
        {assignment.topic, partition}
      end)
    end)
  end

  # `LeaveGroupRequest` is used to voluntarily leave a group. This tells the
  # broker that the member is leaving the group without having to wait for the
  # session timeout to expire. Leaving a group triggers a rebalance for the
  # remaining group members.
  defp leave(
         %State{
           client: client,
           group_name: group_name,
           member_id: member_id
         } = state
       ) do
    leave_group_response = KafkaExAPI.leave_group(client, group_name, member_id)

    case leave_group_response do
      {:ok, :no_error} ->
        Logger.debug(fn -> "Left consumer group #{group_name}" end)

      {:ok, %{error_code: :no_error}} ->
        Logger.debug(fn -> "Left consumer group #{group_name}" end)

      {:ok, %{error_code: error_code}} ->
        Logger.warning(fn ->
          "Received error #{inspect(error_code)}, " <>
            "consumer group manager will exit regardless."
        end)

      {:error, reason} ->
        Logger.warning(fn ->
          "Received error #{inspect(reason)}, " <>
            "consumer group manager will exit regardless."
        end)
    end

    {:ok, state}
  end

  # When instructed that a rebalance is in progress, a group member must rejoin
  # the group with `JoinGroupRequest` (see join/1). To keep the state
  # synchronized during the join/sync phase, each member pauses its consumers
  # and commits its offsets before rejoining the group.
  defp rebalance(%State{} = state) do
    {:ok, state} = stop_heartbeat_timer(state)
    {:ok, state} = stop_consumer(state)
    join(state)
  end

  ### Timer Management

  # Starts a heartbeat process to send heartbeats in the background to keep the
  # consumers active even if it takes a long time to process a batch of
  # messages.
  defp start_heartbeat_timer(%State{} = state) do
    {:ok, timer} = Heartbeat.start_link(state)

    {:ok, %State{state | heartbeat_timer: timer}}
  end

  # Stops the heartbeat process.
  defp stop_heartbeat_timer(%State{heartbeat_timer: nil} = state),
    do: {:ok, state}

  defp stop_heartbeat_timer(%State{heartbeat_timer: heartbeat_timer} = state) do
    if Process.alive?(heartbeat_timer) do
      :gen_server.stop(heartbeat_timer)
    end

    new_state = %State{state | heartbeat_timer: nil}
    {:ok, new_state}
  end

  ### Consumer Management

  # Starts consuming from the member's assigned partitions.
  defp start_consumer(
         %State{
           consumer_module: consumer_module,
           gen_consumer_module: gen_consumer_module,
           consumer_opts: consumer_opts,
           group_name: group_name,
           member_id: member_id,
           generation_id: generation_id,
           supervisor_pid: pid
         } = state,
         assignments
       ) do
    # add member_id and generation_id to the consumer opts
    consumer_opts =
      Keyword.merge(consumer_opts,
        generation_id: generation_id,
        member_id: member_id
      )

    {:ok, consumer_supervisor_pid} =
      ConsumerGroup.start_consumer(
        pid,
        {gen_consumer_module, consumer_module},
        group_name,
        assignments,
        consumer_opts
      )

    state = %{
      state
      | assignments: assignments,
        consumer_supervisor_pid: consumer_supervisor_pid
    }

    {:ok, state}
  end

  # Stops consuming from the member's assigned partitions and commits offsets.
  defp stop_consumer(%State{supervisor_pid: pid} = state) do
    :ok = ConsumerGroup.stop_consumer(pid)

    {:ok, state}
  end

  ### Partition Assignment

  # Queries the Kafka brokers for a list of partitions for the topics of
  # interest to this consumer group. This function returns a list of
  # topic/partition tuples that can be passed to a GenConsumer's
  # `assign_partitions` method.
  defp assignable_partitions(%State{
         client: client,
         topics: topics,
         group_name: group_name
       }) do
    {:ok, cluster_metadata} = KafkaExAPI.metadata(client)

    Enum.flat_map(topics, fn topic ->
      partitions = get_partitions_for_topic(cluster_metadata, topic)

      warn_if_no_partitions(partitions, group_name, topic)

      Enum.map(partitions, fn partition ->
        {topic, partition}
      end)
    end)
  end

  # Extract partition IDs for a topic from cluster metadata
  defp get_partitions_for_topic(cluster_metadata, topic_name) do
    case Enum.find(cluster_metadata.topics, fn t -> t.name == topic_name end) do
      nil -> []
      topic -> Enum.map(topic.partitions, & &1.partition_id)
    end
  end

  defp warn_if_no_partitions([], group_name, topic) do
    Logger.warning(fn ->
      "Consumer group #{group_name} encountered nonexistent topic #{topic}"
    end)
  end

  defp warn_if_no_partitions(_partitions, _group_name, _topic), do: :ok

  # This function is used by the group leader to determine partition
  # assignments during the join/sync phase. `members` is provided to the leader
  # by the coordinating broker in `JoinGroupResponse`. `partitions` is a list
  # of topic/partition tuples, obtained from `assignable_partitions/1`. The
  # return value is a complete list of member assignments in the format needed
  # by `SyncGroupResponse`.
  defp assign_partitions(
         %State{partition_assignment_callback: partition_assignment_callback},
         members,
         partitions
       ) do
    # Delegate partition assignment to GenConsumer module.
    assignments = partition_assignment_callback.(members, partitions)

    # Convert assignments to format expected by Kafka protocol.
    packed_assignments =
      Enum.map(assignments, fn {member, topic_partitions} ->
        {member, pack_assignments(topic_partitions)}
      end)

    assignments_map = Enum.into(packed_assignments, %{})

    # Fill in empty assignments for missing member IDs.
    Enum.map(members, fn member ->
      {member, Map.get(assignments_map, member, [])}
    end)
  end

  # Converts assignments from topic/partition tuples to Kafka's protocol format.
  #
  # Example:
  #
  #   pack_assignments([{"foo", 0}, {"foo", 1}]) #=> [{"foo", [0, 1]}]
  defp pack_assignments(assignments) do
    assignments
    |> Enum.reduce(%{}, fn {topic, partition}, assignments ->
      Map.update(assignments, topic, [partition], &(&1 ++ [partition]))
    end)
    |> Map.to_list()
  end
end
