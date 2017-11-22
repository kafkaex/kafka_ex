defmodule KafkaEx.ConsumerGroup.Manager do
  @moduledoc false

  # actual consumer group management implementation

  use GenServer

  alias KafkaEx.ConsumerGroup
  alias KafkaEx.ConsumerGroup.PartitionAssignment
  alias KafkaEx.Protocol.JoinGroup.Request, as: JoinGroupRequest
  alias KafkaEx.Protocol.JoinGroup.Response, as: JoinGroupResponse
  alias KafkaEx.Protocol.Heartbeat.Request, as: HeartbeatRequest
  alias KafkaEx.Protocol.Heartbeat.Response, as: HeartbeatResponse
  alias KafkaEx.Protocol.LeaveGroup.Request, as: LeaveGroupRequest
  alias KafkaEx.Protocol.Metadata.Response, as: MetadataResponse
  alias KafkaEx.Protocol.SyncGroup.Request, as: SyncGroupRequest
  alias KafkaEx.Protocol.SyncGroup.Response, as: SyncGroupResponse

  require Logger

  defmodule State do
    @moduledoc false
    defstruct [
      :supervisor_pid,
      :worker_name,
      :heartbeat_interval,
      :session_timeout,
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
      :heartbeat_timer,
    ]
  end

  @heartbeat_interval 5_000
  @session_timeout 30_000
  @session_timeout_padding 5_000

  # Client API

  @doc false
  # use `KafkaEx.ConsumerGroup.start_link/4` instead
  @spec start_link(module, binary, [binary], KafkaEx.GenConsumer.options) ::
    GenServer.on_start
  def start_link(consumer_module, group_name, topics, opts \\ []) do
    gen_server_opts = Keyword.get(opts, :gen_server_opts, [])
    consumer_opts = Keyword.drop(opts, [:gen_server_opts])

    GenServer.start_link(
      __MODULE__,
      {consumer_module, group_name, topics, consumer_opts},
      gen_server_opts
    )
  end

  # GenServer callbacks

  def init({consumer_module, group_name, topics, opts}) do
    heartbeat_interval = Keyword.get(
      opts,
      :heartbeat_interval,
      Application.get_env(:kafka_ex, :heartbeat_interval, @heartbeat_interval)
    )
    session_timeout = Keyword.get(
      opts,
      :session_timeout,
      Application.get_env(:kafka_ex, :session_timeout, @session_timeout)
    )
    partition_assignment_callback = Keyword.get(
      opts,
      :partition_assignment_callback,
      &PartitionAssignment.round_robin/2
    )

    supervisor_pid = Keyword.fetch!(opts, :supervisor_pid)
    consumer_opts = Keyword.drop(
      opts,
      [
        :supervisor_pid,
        :heartbeat_interval,
        :session_timeout,
        :partition_assignment_callback
      ]
    )

    worker_opts = Keyword.take(opts, [:uris])
    {:ok, worker_name} = KafkaEx.create_worker(
      :no_name,
      [consumer_group: group_name] ++ worker_opts
    )

    state = %State{
      supervisor_pid: supervisor_pid,
      worker_name: worker_name,
      heartbeat_interval: heartbeat_interval,
      session_timeout: session_timeout,
      consumer_module: consumer_module,
      partition_assignment_callback: partition_assignment_callback,
      consumer_opts: consumer_opts,
      group_name: group_name,
      topics: topics,
      member_id: nil,
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
    :timeout, %State{generation_id: nil, member_id: nil} = state
  ) do
    {:ok, new_state} = join(state)

    {:noreply, new_state}
  end

  # After joining the group, a member must periodically send heartbeats to the
  # group coordinator.
  def handle_info(:heartbeat, %State{} = state) do
    {:ok, new_state} = heartbeat(state)

    {:noreply, new_state}
  end

  # When terminating, inform the group coordinator that this member is leaving
  # the group so that the group can rebalance without waiting for a session
  # timeout.
  def terminate(_reason, %State{generation_id: nil, member_id: nil}), do: :ok
  def terminate(_reason, %State{} = state) do
    :ok = leave(state)
    Process.unlink(state.worker_name)
    KafkaEx.stop_worker(state.worker_name)
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
  # the leader, which is reponsible for assigning partitions to all of the
  # group members. Once a `JoinGroupResponse` is received, all group members
  # must send a `SyncGroupRequest` (see sync/2).

  defp join(
    %State{
      worker_name: worker_name,
      session_timeout: session_timeout,
      group_name: group_name,
      topics: topics,
      member_id: member_id
    } = state
  ) do
    join_request = %JoinGroupRequest{
      group_name: group_name,
      member_id: member_id || "",
      topics: topics,
      session_timeout: session_timeout,
    }

    join_response = KafkaEx.join_group(
      join_request,
      worker_name: worker_name,
      timeout: session_timeout + @session_timeout_padding
    )

    # crash the worker if we recieve an error, but do it with a meaningful
    #   error message
    if join_response.error_code != :no_error do
      raise "Error joining consumer group #{group_name}: " <>
        "#{inspect join_response.error_code}"
    end

    Logger.debug(fn -> "Joined consumer group #{group_name}" end)

    new_state = %State{
      state |
      leader_id: join_response.leader_id,
      member_id: join_response.member_id,
      generation_id: join_response.generation_id
    }

    assignments =
      if JoinGroupResponse.leader?(join_response) do
        # Leader is responsible for assigning partitions to all group members.
        partitions = assignable_partitions(new_state)
        assign_partitions(new_state, join_response.members, partitions)
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
      group_name: group_name,
      member_id: member_id,
      generation_id: generation_id,
      worker_name: worker_name,
      session_timeout: session_timeout
    } = state,
    assignments
  ) do
    sync_request = %SyncGroupRequest{
      group_name: group_name,
      member_id: member_id,
      generation_id: generation_id,
      assignments: assignments,
    }

    sync_group_response = KafkaEx.sync_group(
      sync_request,
      worker_name: worker_name,
      timeout: session_timeout + @session_timeout_padding
    )

    case sync_group_response do
      %SyncGroupResponse{error_code: :no_error, assignments: assignments} ->
        new_state = state
                    |> stop_consumer()
                    |> start_consumer(unpack_assignments(assignments))
                    |> start_heartbeat_timer()
        {:ok, new_state}
      %SyncGroupResponse{error_code: :rebalance_in_progress} ->
        rebalance(state)
    end
  end

  # `HeartbeatRequest` is sent periodically by each active group member (after
  # completing the join/sync phase) to inform the broker that the member is
  # still alive and participating in the group. If a group member fails to send
  # a heartbeat before the group's session timeout expires, the coordinator
  # removes that member from the group and initiates a rebalance.
  #
  # `HeartbeatResponse` allows the coordinating broker to communicate the
  # group's status to each member:
  #
  #   * `:no_error` indicates that the group is up to date and no action is
  #   needed.  * `:rebalance_in_progress` instructs each member to rejoin the
  #   group by sending a `JoinGroupRequest` (see join/1).
  defp heartbeat(
    %State{
      worker_name: worker_name,
      group_name: group_name,
      generation_id: generation_id,
      member_id: member_id
    } = state
  ) do
    heartbeat_request = %HeartbeatRequest{
      group_name: group_name,
      member_id: member_id,
      generation_id: generation_id,
    }

    case KafkaEx.heartbeat(heartbeat_request, worker_name: worker_name) do
      %HeartbeatResponse{error_code: :no_error} ->
        new_state = start_heartbeat_timer(state)
        {:ok, new_state}
      %HeartbeatResponse{error_code: :rebalance_in_progress} ->
        Logger.debug(fn -> "Rebalancing consumer group #{group_name}" end)
        rebalance(state)
    end
  end

  # `LeaveGroupRequest` is used to voluntarily leave a group. This tells the
  # broker that the member is leaving the group without having to wait for the
  # session timeout to expire. Leaving a group triggers a rebalance for the
  # remaining group members.
  defp leave(
    %State{
      worker_name: worker_name,
      group_name: group_name,
      member_id: member_id
    } = state
  ) do
    stop_heartbeat_timer(state)

    leave_request = %LeaveGroupRequest{
      group_name: group_name,
      member_id: member_id,
    }

    leave_group_response =
      KafkaEx.leave_group(leave_request, worker_name: worker_name)

    if leave_group_response.error_code == :no_error do
      Logger.debug(fn -> "Left consumer group #{group_name}" end)
    else
      Logger.warn(fn ->
        "Received error #{inspect leave_group_response.error_code}, " <>
        "consumer group manager will exit regardless."
      end)
    end

    :ok
  end

  # When instructed that a rebalance is in progress, a group member must rejoin
  # the group with `JoinGroupRequest` (see join/1). To keep the state
  # synchronized during the join/sync phase, each member pauses its consumers
  # and commits its offsets before rejoining the group.
  defp rebalance(%State{} = state) do
    state
    |> stop_heartbeat_timer()
    |> stop_consumer()
    |> join()
  end

  ### Timer Management

  # Starts a timer for the next heartbeat.
  defp start_heartbeat_timer(
    %State{heartbeat_interval: heartbeat_interval} = state
  ) do
    {:ok, timer} = :timer.send_after(heartbeat_interval, :heartbeat)

    %State{state | heartbeat_timer: timer}
  end

  # Stops any active heartbeat timer.
  defp stop_heartbeat_timer(%State{heartbeat_timer: nil} = state), do: state
  defp stop_heartbeat_timer(
    %State{heartbeat_timer: heartbeat_timer} = state
  ) do
    {:ok, :cancel} = :timer.cancel(heartbeat_timer)

    %State{state | heartbeat_timer: nil}
  end

  ### Consumer Management

  # Starts consuming from the member's assigned partitions.
  defp start_consumer(
    %State{
      consumer_module: consumer_module,
      consumer_opts: consumer_opts,
      group_name: group_name,
      supervisor_pid: pid
    } = state,
    assignments
  ) do
    {:ok, consumer_supervisor_pid} = ConsumerGroup.start_consumer(
      pid,
      consumer_module,
      group_name,
      assignments,
      consumer_opts
    )

    %{
      state |
      assignments: assignments,
      consumer_supervisor_pid: consumer_supervisor_pid
    }
  end

  # Stops consuming from the member's assigned partitions and commits offsets.
  defp stop_consumer(%State{supervisor_pid: pid} = state) do
    :ok = ConsumerGroup.stop_consumer(pid)

    state
  end

  ### Partition Assignment

  # Queries the Kafka brokers for a list of partitions for the topics of
  # interest to this consumer group. This function returns a list of
  # topic/partition tuples that can be passed to a GenConsumer's
  # `assign_partitions` method.
  defp assignable_partitions(
    %State{worker_name: worker_name, topics: topics, group_name: group_name}
  ) do
    metadata = KafkaEx.metadata(worker_name: worker_name)

    Enum.flat_map(topics, fn (topic) ->
      partitions = MetadataResponse.partitions_for_topic(metadata, topic)

      warn_if_no_partitions(partitions, group_name, topic)

      Enum.map(partitions, fn (partition) ->
        {topic, partition}
      end)
    end)
  end

  defp warn_if_no_partitions([], group_name, topic) do
    Logger.warn(fn ->
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
      Enum.map(assignments, fn ({member, topic_partitions}) ->
        {member, pack_assignments(topic_partitions)}
      end)
    assignments_map = Enum.into(packed_assignments, %{})

    # Fill in empty assignments for missing member IDs.
    Enum.map(members, fn (member) ->
      {member, Map.get(assignments_map, member, [])}
    end)
  end

  # Converts assignments from Kafka's protocol format to topic/partition tuples.
  #
  # Example:
  #
  #   unpack_assignments([{"foo", [0, 1]}]) #=> [{"foo", 0}, {"foo", 1}]
  defp unpack_assignments(assignments) do
    Enum.flat_map(assignments, fn ({topic, partition_ids}) ->
      Enum.map(partition_ids, &({topic, &1}))
    end)
  end

  # Converts assignments from topic/partition tuples to Kafka's protocol format.
  #
  # Example:
  #
  #   pack_assignments([{"foo", 0}, {"foo", 1}]) #=> [{"foo", [0, 1]}]
  defp pack_assignments(assignments) do
    assignments
    |> Enum.reduce(%{}, fn({topic, partition}, assignments) ->
      Map.update(assignments, topic, [partition], &(&1 ++ [partition]))
    end)
    |> Map.to_list
  end
end
