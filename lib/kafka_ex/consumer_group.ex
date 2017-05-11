defmodule KafkaEx.ConsumerGroup do
  @moduledoc """
  A process that manages membership in a Kafka consumer group.

  Consumers in a consumer group coordinate with each other through a Kafka broker to distribute the
  work of consuming one or several topics without any overlap. This is facilitated by the [Kafka
  client-side assignment
  protocol](https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Client-side+Assignment+Proposal).

  Any time group membership changes (a member joins or leaves the group), a Kafka broker initiates
  group synchronization by asking one of the group members (the leader) to provide partition
  assignments for the whole group. Partition assignment is handled by the
  `c:KafkaEx.GenConsumer.assign_partitions/2` callback of the provided consumer module.

  A `ConsumerGroup` process is responsible for:

  1. Maintaining membership in a Kafka consumer group.
  2. Determining partition assignments if elected as the group leader.
  3. Launching and terminating `GenConsumer` processes based on its assigned partitions.

  To use a `ConsumerGroup`, a developer must define a module that implements the
  `KafkaEx.GenConsumer` behaviour and start a `ConsumerGroup` with that module.

  ## Example

  The following consumer prints each message with the name of the node that's consuming the message:

  ```
  defmodule DistributedConsumer do
    use KafkaEx.GenConsumer

    def handle_message(%Message{value: message}, state) do
      IO.puts(to_string(node()) <> ": " <> inspect(message))
      {:ack, state}
    end
  end

  # use DistributedConsumer in a consumer group
  {:ok, pid} = KafkaEx.ConsumerGroup.start_link(DistributedConsumer, "test_group", ["test_topic"])
  ```

  Running this on multiple nodes might display the following:

  ```txt
  node1@host: "messages"
  node2@host: "on"
  node2@host: "multiple"
  node1@host: "nodes"
  ```

  It is not necessary for the nodes to be connected, because `ConsumerGroup` uses Kafka's built-in
  group coordination protocol.
  """

  use GenServer

  alias KafkaEx.Config
  alias KafkaEx.Protocol.JoinGroup.Request, as: JoinGroupRequest
  alias KafkaEx.Protocol.JoinGroup.Response, as: JoinGroupResponse
  alias KafkaEx.Protocol.Heartbeat.Request, as: HeartbeatRequest
  alias KafkaEx.Protocol.Heartbeat.Response, as: HeartbeatResponse
  alias KafkaEx.Protocol.LeaveGroup.Request, as: LeaveGroupRequest
  alias KafkaEx.Protocol.LeaveGroup.Response, as: LeaveGroupResponse
  alias KafkaEx.Protocol.Metadata.Response, as: MetadataResponse
  alias KafkaEx.Protocol.Metadata.TopicMetadata
  alias KafkaEx.Protocol.Metadata.PartitionMetadata
  alias KafkaEx.Protocol.SyncGroup.Request, as: SyncGroupRequest
  alias KafkaEx.Protocol.SyncGroup.Response, as: SyncGroupResponse

  require Logger

  defmodule State do
    @moduledoc false
    defstruct [
      :worker_name,
      :heartbeat_interval,
      :session_timeout,
      :consumer_module,
      :consumer_opts,
      :group_name,
      :topics,
      :member_id,
      :generation_id,
      :assignments,
      :consumer_pid,
    ]
  end

  @heartbeat_interval 5_000
  @session_timeout 30_000
  @session_timeout_padding 5_000

  # Client API

  @doc """
  Starts a `ConsumerGroup` process linked to the current process.

  This can be used to start a `ConsumerGroup` as part of a supervision tree.

  `module` is a module that implements the `KafkaEx.GenConsumer` behaviour. `group_name` is the name
  of the consumer group. `topics` is a list of topics that the consumer group should consume from.
  `opts` can be any options accepted by `GenConsumer` or `GenServer`.

  ### Return Values

  This function has the same return values as `GenServer.start_link/3`.

  If the consumer group is successfully created and initialized, this function returns `{:ok, pid}`,
  where `pid` is the PID of the consumer group process.
  """
  @spec start_link(module, binary, [binary], KafkaEx.GenConsumer.options) :: GenServer.on_start
  def start_link(consumer_module, group_name, topics, opts \\ []) do
    {server_opts, consumer_opts} = Keyword.split(opts, [:debug, :name, :timeout, :spawn_opt])

    GenServer.start_link(__MODULE__, {consumer_module, group_name, topics, consumer_opts}, server_opts)
  end

  # GenServer callbacks

  def init({consumer_module, group_name, topics, opts}) do
    worker_name = Keyword.get(opts, :worker_name, Config.default_worker)
    heartbeat_interval = Keyword.get(opts, :heartbeat_interval, Application.get_env(:kafka_ex, :heartbeat_interval, @heartbeat_interval))
    session_timeout = Keyword.get(opts, :session_timeout, Application.get_env(:kafka_ex, :session_timeout, @session_timeout))

    consumer_opts = Keyword.drop(opts, [:heartbeat_interval, :session_timeout])

    state = %State{
      worker_name: worker_name,
      heartbeat_interval: heartbeat_interval,
      session_timeout: session_timeout,
      consumer_module: consumer_module,
      consumer_opts: consumer_opts,
      group_name: group_name,
      topics: topics,
      member_id: "",
    }

    Process.flag(:trap_exit, true)

    {:ok, state, 0}
  end

  # If `member_id` and `generation_id` aren't set, we haven't yet joined the group. `member_id` and
  # `generation_id` are initialized by `JoinGroupResponse`.
  def handle_info(:timeout, %State{generation_id: nil, member_id: ""} = state) do
    {:ok, new_state} = join(state)

    {:noreply, new_state, new_state.heartbeat_interval}
  end

  # After joining the group, a member must periodically send heartbeats to the group coordinator.
  def handle_info(:timeout, %State{} = state) do
    {:ok, new_state} = heartbeat(state)

    {:noreply, new_state, new_state.heartbeat_interval}
  end

  # Stop the consumer group if the consumer supervisor crashes.
  def handle_info({:EXIT, pid, reason}, %State{consumer_pid: pid} = state) do
    new_state = %State{state | consumer_pid: nil}

    {:stop, reason, new_state}
  end

  # Ignore spurious exit signals. These are often from old consumer PIDs, because the exit signal
  # can arrive after the `consumer_pid` state has already been updated.
  def handle_info({:EXIT, _pid, _reason}, %State{} = state) do
    {:noreply, state, state.heartbeat_interval}
  end

  # When terminating, inform the group coordinator that this member is leaving the group so that the
  # group can rebalance without waiting for a session timeout.
  def terminate(_reason, %State{generation_id: nil, member_id: ""}), do: :ok
  def terminate(_reason, %State{} = state) do
    :ok = leave(state)
  end

  ### Helpers

  # `JoinGroupRequest` is used to set the active members of a group. The response blocks until the
  # broker has decided that it has a full list of group members. This requires that all active
  # members send a `JoinGroupRequest`. For active members, this is triggered by the broker
  # responding to a heartbeat with a `:rebalance_in_progress` error code. If any group members fail
  # to send a `JoinGroupRequest` before the session timeout expires, then those group members are
  # removed from the group and synchronization continues without them.
  #
  # `JoinGroupResponse` tells each member its unique member ID as well as the group's current
  # generation ID. The broker will pick one group member to be the leader, which is reponsible for
  # assigning partitions to all of the group members. Once a `JoinGroupResponse` is received, all
  # group members must send a `SyncGroupRequest` (see sync/2).
  defp join(%State{worker_name: worker_name, session_timeout: session_timeout, group_name: group_name, topics: topics, member_id: member_id} = state) do
    join_request = %JoinGroupRequest{
      group_name: group_name,
      member_id: member_id,
      topics: topics,
      session_timeout: session_timeout,
    }

    join_response = %JoinGroupResponse{error_code: :no_error} =
      KafkaEx.join_group(join_request, worker_name: worker_name, timeout: session_timeout + @session_timeout_padding)

    Logger.debug("Joined consumer group #{group_name}")

    new_state = %State{state | member_id: join_response.member_id, generation_id: join_response.generation_id}

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

  # `SyncGroupRequest` is used to distribute partition assignments to all group members. All group
  # members must send this request after receiving a response to a `JoinGroupRequest`. The request
  # blocks until assignments are provided by the leader. The leader sends partition assignments
  # (given by the `assignments` parameter) as part of its `SyncGroupRequest`. For all other members,
  # `assignments` must be empty.
  #
  # `SyncGroupResponse` contains the individual member's partition assignments. Upon receiving a
  # successful `SyncGroupResponse`, a group member is free to start consuming from its assigned
  # partitions, but must send periodic heartbeats to the coordinating broker.
  defp sync(%State{group_name: group_name, member_id: member_id, generation_id: generation_id,
                   worker_name: worker_name, session_timeout: session_timeout} = state, assignments) do
    sync_request = %SyncGroupRequest{
      group_name: group_name,
      member_id: member_id,
      generation_id: generation_id,
      assignments: assignments,
    }

    case KafkaEx.sync_group(sync_request, worker_name: worker_name, timeout: session_timeout + @session_timeout_padding) do
      %SyncGroupResponse{error_code: :no_error, assignments: assignments} ->
        start_consumer(state, unpack_assignments(assignments))

      %SyncGroupResponse{error_code: :rebalance_in_progress} ->
        rebalance(state)
    end
  end

  # `HeartbeatRequest` is sent periodically by each active group member (after completing the
  # join/sync phase) to inform the broker that the member is still alive and participating in the
  # group. If a group member fails to send a heartbeat before the group's session timeout expires,
  # the coordinator removes that member from the group and initiates a rebalance.
  #
  # `HeartbeatResponse` allows the coordinating broker to communicate the group's status to each
  # member:
  #
  #   * `:no_error` indicates that the group is up to date and no action is needed.
  #   * `:rebalance_in_progress` instructs each member to rejoin the group by sending a
  #     `JoinGroupRequest` (see join/1).
  defp heartbeat(%State{worker_name: worker_name, group_name: group_name, generation_id: generation_id, member_id: member_id} = state) do
    heartbeat_request = %HeartbeatRequest{
      group_name: group_name,
      member_id: member_id,
      generation_id: generation_id,
    }

    case KafkaEx.heartbeat(heartbeat_request, worker_name: worker_name) do
      %HeartbeatResponse{error_code: :no_error} ->
        {:ok, state}

      %HeartbeatResponse{error_code: :rebalance_in_progress} ->
        Logger.debug("Rebalancing consumer group #{group_name}")
        rebalance(state)
    end
  end

  # `LeaveGroupRequest` is used to voluntarily leave a group. This tells the broker that the member
  # is leaving the group without having to wait for the session timeout to expire. Leaving a group
  # triggers a rebalance for the remaining group members.
  defp leave(%State{worker_name: worker_name, group_name: group_name, member_id: member_id} = state) do
    stop_consumer(state)

    leave_request = %LeaveGroupRequest{
      group_name: group_name,
      member_id: member_id,
    }

    %LeaveGroupResponse{error_code: :no_error} = KafkaEx.leave_group(leave_request, worker_name: worker_name)

    Logger.debug("Left consumer group #{group_name}")

    :ok
  end

  # When instructed that a rebalance is in progress, a group member must rejoin the group with
  # `JoinGroupRequest` (see join/1). To keep the state synchronized during the join/sync phase, each
  # member pauses its consumers and commits its offsets before rejoining the group.
  defp rebalance(%State{} = state) do
    state
    |> stop_consumer()
    |> join()
  end

  ### Consumer Management

  # Starts consuming from the member's assigned partitions.
  defp start_consumer(%State{consumer_module: consumer_module, consumer_opts: consumer_opts,
                             group_name: group_name, consumer_pid: nil} = state, assignments) do
    {:ok, pid} = KafkaEx.GenConsumer.Supervisor.start_link(consumer_module, group_name, assignments, consumer_opts)

    {:ok, %State{state | assignments: assignments, consumer_pid: pid}}
  end

  # Stops consuming from the member's assigned partitions and commits offsets.
  defp stop_consumer(%State{consumer_pid: nil} = state), do: state
  defp stop_consumer(%State{consumer_pid: pid} = state) when is_pid(pid) do
    :ok = Supervisor.stop(pid)
    %State{state | consumer_pid: nil}
  end

  ### Partition Assignment

  # Queries the Kafka brokers for a list of partitions for the topics of interest to this consumer
  # group. This function returns a list of topic/partition tuples that can be passed to a
  # GenConsumer's `assign_partitions` method.
  defp assignable_partitions(%State{worker_name: worker_name, topics: topics}) do
    %MetadataResponse{topic_metadatas: topic_metadatas} = KafkaEx.metadata(worker_name: worker_name)

    Enum.flat_map(topics, fn (topic) ->
      %TopicMetadata{error_code: :no_error, partition_metadatas: partition_metadatas} = Enum.find(topic_metadatas, &(&1.topic == topic))

      Enum.map(partition_metadatas, fn (%PartitionMetadata{error_code: :no_error, partition_id: partition_id}) ->
        {topic, partition_id}
      end)
    end)
  end

  # This function is used by the group leader to determine partition assignments during the
  # join/sync phase. `members` is provided to the leader by the coordinating broker in
  # `JoinGroupResponse`. `partitions` is a list of topic/partition tuples, obtained from
  # `assignable_partitions/1`. The return value is a complete list of member assignments in the
  # format needed by `SyncGroupResponse`.
  defp assign_partitions(%State{consumer_module: consumer_module}, members, partitions) do
    # Delegate partition assignment to GenConsumer module.
    assignments = consumer_module.assign_partitions(members, partitions)

    # Convert assignments to format expected by Kafka protocol.
    packed_assignments =
      Enum.map(assignments, fn ({member, topic_partitions}) ->
        {member, pack_assignments(topic_partitions)}
      end)
    assignments_map = Map.new(packed_assignments)

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
    |> Enum.group_by(&(elem(&1, 0)), &(elem(&1, 1)))
    |> Enum.into([])
  end
end
