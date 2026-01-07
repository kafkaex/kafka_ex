defmodule KafkaEx.Consumer.ConsumerGroup.Manager do
  @moduledoc """
  Manages membership in a Kafka consumer group.

  This module implements the Kafka consumer group protocol, handling:

  - **Group Membership**: Joining and leaving consumer groups via JoinGroup/LeaveGroup requests
  - **Partition Assignment**: Coordinating partition distribution among group members
  - **Rebalancing**: Triggering rebalances when members join/leave or partitions change
  - **Heartbeats**: Maintaining group membership through periodic heartbeat messages

  ## Consumer Group Protocol Flow

  1. **Join Phase**: Send `JoinGroupRequest` to group coordinator. The coordinator
     blocks until all members have joined, then elects a leader.

  2. **Sync Phase**: The leader computes partition assignments and sends them via
     `SyncGroupRequest`. All members receive their assignments in the response.

  3. **Consume Phase**: Members consume from assigned partitions while sending
     periodic heartbeats to maintain membership.

  4. **Rebalance**: When the coordinator signals a rebalance (via heartbeat response),
     members stop consuming, commit offsets, and rejoin the group.

  ## Options

  The following options can be passed when starting a consumer group:

  - `:heartbeat_interval` - Interval between heartbeats in ms (default: 5000)
  - `:session_timeout` - Session timeout in ms (default: 30000)
  - `:session_timeout_padding` - Extra time added to request timeouts (default: 10000)
  - `:rebalance_timeout` - Time allowed for consumers to rejoin during rebalance (default: `session_timeout * 3`)
  - `:partition_assignment_callback` - Function for custom partition assignment (default: round-robin)

  Options can also be configured globally via application config under `:kafka_ex`.
  """
  use GenServer

  alias KafkaEx.API, as: KafkaExAPI
  alias KafkaEx.Client
  alias KafkaEx.Config
  alias KafkaEx.Consumer.ConsumerGroup
  alias KafkaEx.Consumer.ConsumerGroup.Heartbeat
  alias KafkaEx.Consumer.ConsumerGroup.PartitionAssignment
  alias KafkaEx.Telemetry
  require Logger

  defmodule State do
    @moduledoc false
    defstruct [
      :supervisor_pid,
      :client,
      :heartbeat_interval,
      :session_timeout,
      :session_timeout_padding,
      :rebalance_timeout,
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

    @type t :: %__MODULE__{
            supervisor_pid: pid(),
            client: pid(),
            heartbeat_interval: pos_integer(),
            session_timeout: pos_integer(),
            session_timeout_padding: pos_integer(),
            rebalance_timeout: pos_integer(),
            gen_consumer_module: module(),
            consumer_module: module(),
            consumer_opts: Keyword.t(),
            partition_assignment_callback: function(),
            group_name: binary(),
            topics: [binary()],
            member_id: binary() | nil,
            leader_id: binary() | nil,
            consumer_supervisor_pid: pid() | nil,
            members: [binary()] | nil,
            generation_id: integer() | nil,
            assignments: [{binary(), integer()}] | nil,
            heartbeat_timer: reference() | nil
          }
  end

  @heartbeat_interval 5_000
  @session_timeout 30_000
  @session_timeout_padding 10_000
  # Default rebalance_timeout multiplier (relative to session_timeout)
  # In Java client, rebalance_timeout = max.poll.interval.ms (default 5 min)
  # We use session_timeout * 3 as reasonable default
  @rebalance_timeout_multiplier 3
  @max_join_retries 6

  # Gets a value from opts, falling back to application config, then to default
  defp get_with_default(opts, key, default) do
    Keyword.get(opts, key, Application.get_env(:kafka_ex, key, default))
  end

  @type assignments :: [{binary(), integer()}]

  # Client API

  @doc false
  @spec start_link({{module, module}, binary, [binary], Keyword.t()}) :: GenServer.on_start()
  def start_link({{gen_consumer_module, consumer_module}, group_name, topics, opts}) do
    gen_server_opts = Keyword.get(opts, :gen_server_opts, [])
    consumer_opts = Keyword.drop(opts, [:gen_server_opts])
    opts = {{gen_consumer_module, consumer_module}, group_name, topics, consumer_opts}
    GenServer.start_link(__MODULE__, opts, gen_server_opts)
  end

  # GenServer callbacks

  # Dialyzer reports no_return because KafkaEx.Client.start_link can raise on init failure.
  # However, the function does return normally when client starts successfully.
  @dialyzer {:nowarn_function, init: 1}
  def init({{gen_consumer_module, consumer_module}, group_name, topics, opts}) do
    heartbeat_interval = get_with_default(opts, :heartbeat_interval, @heartbeat_interval)
    session_timeout = get_with_default(opts, :session_timeout, @session_timeout)
    session_timeout_padding = get_with_default(opts, :session_timeout_padding, @session_timeout_padding)
    rebalance_timeout = get_with_default(opts, :rebalance_timeout, session_timeout * @rebalance_timeout_multiplier)

    partition_assignment_callback =
      Keyword.get(opts, :partition_assignment_callback, &PartitionAssignment.round_robin/2)

    supervisor_pid = Keyword.fetch!(opts, :supervisor_pid)

    consumer_opts =
      Keyword.drop(opts, [
        :supervisor_pid,
        :heartbeat_interval,
        :session_timeout,
        :session_timeout_padding,
        :rebalance_timeout,
        :partition_assignment_callback
      ])

    # Use Config defaults for connection options if not provided
    client_opts =
      [
        uris: Keyword.get(opts, :uris) || Config.brokers(),
        use_ssl: Keyword.get(opts, :use_ssl, Config.use_ssl()),
        ssl_options: Keyword.get(opts, :ssl_options, Config.ssl_options()),
        auth: Keyword.get(opts, :auth) || Config.auth_config(),
        consumer_group: group_name,
        initial_topics: topics
      ]

    case Client.start_link(client_opts, :no_name) do
      {:ok, client} ->
        state = %State{
          supervisor_pid: supervisor_pid,
          client: client,
          heartbeat_interval: heartbeat_interval,
          session_timeout: session_timeout,
          session_timeout_padding: session_timeout_padding,
          rebalance_timeout: rebalance_timeout,
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

      {:error, reason} ->
        {:stop, reason}

      :ignore ->
        :ignore
    end
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
  def handle_info(:timeout, %State{generation_id: nil, member_id: nil} = state) do
    {:ok, new_state} = join(state)
    {:noreply, new_state}
  end

  # If the heartbeat gets an error, we need to rebalance.
  def handle_info({:EXIT, timer, {:shutdown, :rebalance}}, %State{heartbeat_timer: timer} = state) do
    {:ok, state} = rebalance(state, :heartbeat_timeout)
    {:noreply, state}
  end

  # If the heartbeat gets an unrecoverable error.
  def handle_info({:EXIT, _heartbeat_timer, {:shutdown, {:error, reason}}}, %State{} = state) do
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
           rebalance_timeout: rebalance_timeout,
           group_name: group_name,
           topics: topics,
           member_id: member_id
         } = state,
         attempt_number
       ) do
    opts = [
      topics: topics,
      session_timeout: session_timeout,
      rebalance_timeout: rebalance_timeout,
      timeout: session_timeout + session_timeout_padding
    ]

    join_response = KafkaExAPI.join_group(client, group_name, member_id || "", opts)

    # crash the client if we receive an error, but do it with a meaningful
    # error message
    case join_response do
      {:ok, %KafkaEx.Messages.JoinGroup{} = response} ->
        on_successful_join(state, response)

      {:error, :no_broker} ->
        if attempt_number >= @max_join_retries do
          raise "Unable to join consumer group #{state.group_name} after #{@max_join_retries} attempts"
        end

        Logger.warning(
          "Unable to join consumer group #{inspect(group_name)}.  " <>
            "Will sleep 1 second and try again (attempt number #{attempt_number})"
        )

        :timer.sleep(1000)
        join(state, attempt_number + 1)

      {:error, reason} ->
        raise "Error joining consumer group #{group_name}: #{inspect(reason)}"
    end
  end

  defp on_successful_join(state, join_response) do
    Logger.debug(
      "Joined consumer group #{state.group_name} generation:#{join_response.generation_id}-#{join_response.member_id}"
    )

    new_state = %State{
      state
      | leader_id: join_response.leader_id,
        member_id: join_response.member_id,
        generation_id: join_response.generation_id
    }

    is_leader = join_response.leader_id == join_response.member_id

    assignments =
      if is_leader do
        partitions = assignable_partitions(new_state)
        member_ids = Enum.map(join_response.members, & &1.member_id)
        assign_partitions(new_state, member_ids, partitions)
      else
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
    # Convert assignments to protocol-agnostic format (protocol layer handles Kayrock conversion)
    group_assignment = format_assignments_for_sync_group(assignments)
    opts = [group_assignment: group_assignment, timeout: session_timeout + session_timeout_padding]

    case KafkaExAPI.sync_group(client, group_name, generation_id, member_id, opts) do
      {:ok, sync_response} ->
        {:ok, state} = start_heartbeat_timer(state)
        {:ok, state} = stop_consumer(state)
        consumer_assignments = extract_consumer_assignments(sync_response.partition_assignments)
        start_consumer(state, consumer_assignments)

      {:error, :rebalance_in_progress} ->
        rebalance(state, :rebalance_in_progress)

      {:error, reason} ->
        raise "Error syncing consumer group #{group_name}: #{inspect(reason)}"
    end
  end

  # Convert assignments from Manager format to protocol-agnostic format for sync_group request
  # Input: [{member_id, [{topic, [partition_ids]}]}]
  # Output: [%{member_id: ..., topic_partitions: [{topic, [partitions]}]}]
  # The protocol layer handles conversion to Kayrock structs
  defp format_assignments_for_sync_group(assignments) do
    Enum.map(assignments, fn {member_id, topic_partitions} ->
      %{
        member_id: member_id,
        topic_partitions: topic_partitions
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
  defp leave(%State{client: client, group_name: group_name, member_id: member_id} = state) do
    case KafkaExAPI.leave_group(client, group_name, member_id) do
      {:ok, %KafkaEx.Messages.LeaveGroup{}} ->
        Logger.debug("Left consumer group #{group_name}")

      {:error, reason} ->
        Logger.warning("Received error #{inspect(reason)}, consumer group manager will exit regardless.")
    end

    {:ok, state}
  end

  # When instructed that a rebalance is in progress, a group member must rejoin
  # the group with `JoinGroupRequest` (see join/1). To keep the state
  # synchronized during the join/sync phase, each member pauses its consumers
  # and commits its offsets before rejoining the group.
  defp rebalance(%State{} = state, reason) do
    Telemetry.emit_rebalance(
      state.group_name,
      state.member_id || "",
      state.generation_id,
      reason
    )

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

  defp stop_heartbeat_timer(%State{heartbeat_timer: nil} = state), do: {:ok, state}

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
    consumer_opts = Keyword.merge(consumer_opts, generation_id: generation_id, member_id: member_id)

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
    case Map.get(cluster_metadata.topics, topic_name) do
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
  defp assign_partitions(%State{partition_assignment_callback: partition_assignment_callback}, members, partitions) do
    # Delegate partition assignment to GenConsumer module.
    assignments = Map.new(partition_assignment_callback.(members, partitions))

    # Convert assignments to protocol format, filling in empty assignments for missing members.
    Enum.map(members, fn member ->
      {member, pack_assignments(Map.get(assignments, member, []))}
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
