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
      :partitions,
      :member_id,
      :generation_id,
      :assignments,
      :consumer_pid,
    ]
  end

  @heartbeat_interval 5_000
  @session_timeout 30_000

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

  def handle_info(:timeout, %State{generation_id: nil, member_id: ""} = state) do
    new_state = join(state)

    {:noreply, new_state, new_state.heartbeat_interval}
  end

  def handle_info(:timeout, %State{} = state) do
    new_state = heartbeat(state)

    {:noreply, new_state, new_state.heartbeat_interval}
  end

  def handle_info({:EXIT, pid, reason}, %State{consumer_pid: pid} = state) do
    new_state = %State{state | consumer_pid: nil}

    {:stop, reason, new_state}
  end

  def handle_info({:EXIT, _pid, _reason}, %State{} = state) do
    {:noreply, state, state.heartbeat_interval}
  end

  def terminate(_reason, %State{} = state) do
    leave(state)
  end

  # Helpers

  defp join(%State{worker_name: worker_name, session_timeout: session_timeout, group_name: group_name, topics: topics, member_id: member_id} = state) do
    join_request = %JoinGroupRequest{
      group_name: group_name,
      member_id: member_id,
      topics: topics,
      session_timeout: session_timeout,
    }

    join_response = KafkaEx.join_group(join_request, worker_name: worker_name, timeout: session_timeout + 5000)
    new_state = %State{state | member_id: join_response.member_id, generation_id: join_response.generation_id}

    Logger.debug("Joined consumer group #{group_name}")

    if join_response.member_id == join_response.leader_id do
      sync_leader(new_state, join_response.members)
    else
      sync_follower(new_state)
    end
  end

  defp sync_leader(%State{worker_name: worker_name, topics: topics, partitions: nil} = state, members) do
    %MetadataResponse{topic_metadatas: topic_metadatas} = KafkaEx.metadata(worker_name: worker_name)

    partitions = Enum.flat_map(topics, fn (topic) ->
      %TopicMetadata{error_code: :no_error, partition_metadatas: partition_metadatas} = Enum.find(topic_metadatas, &(&1.topic == topic))

      Enum.map(partition_metadatas, fn (%PartitionMetadata{error_code: :no_error, partition_id: partition_id}) ->
        {topic, partition_id}
      end)
    end)

    sync_leader(%State{state | partitions: partitions}, members)
  end

  defp sync_leader(%State{worker_name: worker_name, session_timeout: session_timeout,
                          group_name: group_name, generation_id: generation_id, member_id: member_id} = state, members) do
    assignments = assign_partitions(state, members)

    sync_request = %SyncGroupRequest{
      group_name: group_name,
      member_id: member_id,
      generation_id: generation_id,
      assignments: assignments,
    }

    sync_request
    |> KafkaEx.sync_group(worker_name: worker_name, timeout: session_timeout + 5000)
    |> update_assignments(state)
  end

  defp sync_follower(%State{worker_name: worker_name, session_timeout: session_timeout,
                            group_name: group_name, generation_id: generation_id, member_id: member_id} = state) do
    sync_request = %SyncGroupRequest{
      group_name: group_name,
      member_id: member_id,
      generation_id: generation_id,
      assignments: [],
    }

    sync_request
    |> KafkaEx.sync_group(timeout: session_timeout + 5000, worker_name: worker_name)
    |> update_assignments(state)
  end

  defp update_assignments(%SyncGroupResponse{error_code: :rebalance_in_progress}, %State{} = state), do: rebalance(state)
  defp update_assignments(%SyncGroupResponse{error_code: :no_error, assignments: assignments}, %State{} = state) do
    start_consumer(state, assignments)
  end

  defp heartbeat(%State{worker_name: worker_name, group_name: group_name, generation_id: generation_id, member_id: member_id} = state) do
    heartbeat_request = %HeartbeatRequest{
      group_name: group_name,
      member_id: member_id,
      generation_id: generation_id,
    }

    case KafkaEx.heartbeat(heartbeat_request, worker_name: worker_name) do
      %HeartbeatResponse{error_code: :no_error} ->
        state

      %HeartbeatResponse{error_code: :rebalance_in_progress} ->
        rebalance(state)
    end
  end

  defp rebalance(%State{} = state) do
    state
    |> stop_consumer()
    |> join()
  end

  defp leave(%State{worker_name: worker_name, group_name: group_name, member_id: member_id} = state) do
    stop_consumer(state)

    leave_request = %LeaveGroupRequest{
      group_name: group_name,
      member_id: member_id,
    }

    %LeaveGroupResponse{error_code: :no_error} = KafkaEx.leave_group(leave_request, worker_name: worker_name)

    Logger.debug("Left consumer group #{group_name}")
  end

  defp start_consumer(%State{consumer_module: consumer_module, consumer_opts: consumer_opts,
                             group_name: group_name, consumer_pid: nil} = state, assignments) do
    assignments =
      Enum.flat_map(assignments, fn ({topic, partition_ids}) ->
        Enum.map(partition_ids, &({topic, &1}))
      end)

    {:ok, pid} = KafkaEx.GenConsumer.Supervisor.start_link(consumer_module, group_name, assignments, consumer_opts)

    %State{state | assignments: assignments, consumer_pid: pid}
  end

  defp stop_consumer(%State{consumer_pid: nil} = state), do: state
  defp stop_consumer(%State{consumer_pid: pid} = state) when is_pid(pid) do
    :ok = Supervisor.stop(pid)
    %State{state | consumer_pid: nil}
  end

  defp assign_partitions(%State{consumer_module: consumer_module, partitions: partitions}, members) do
    assignments =
      consumer_module.assign_partitions(members, partitions)
      |> Enum.map(fn ({member, topic_partitions}) ->
        assigns =
          topic_partitions
          |> Enum.group_by(&(elem(&1, 0)), &(elem(&1, 1)))
          |> Enum.into([])

        {member, assigns}
      end)
      |> Map.new

    Enum.map(members, fn (member) ->
      {member, Map.get(assignments, member, [])}
    end)
  end
end
