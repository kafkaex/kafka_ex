defmodule KafkaEx.Test.CapturingMockClient do
  @moduledoc """
  A consumer-group mock client that records the full `opts` of every
  join/sync/heartbeat/leave call, so tests can assert what the Manager and
  Heartbeat actually threaded through (e.g. `group_instance_id`). Unlike
  `KafkaEx.Test.MockClient`, it keeps the opts (that mock drops them) and returns
  benign success structs so a driven Manager/Heartbeat keeps running.

  ## Reading calls after the client is stopped

  `KafkaEx.Consumer.ConsumerGroup.Manager.terminate/2` calls
  `GenServer.stop(client)` on its client, so a test asserting on calls *after*
  a terminate cannot use `calls/1` (the process is gone). Register a watcher
  with `set_watcher/2`; on terminate the mock sends `{:captured_calls, calls}`
  to it, which the test can `receive`.

      {:ok, client} = CapturingMockClient.start_link()
      CapturingMockClient.set_watcher(client, self())
      # ... drive Manager.terminate/2 with this client ...
      assert_receive {:captured_calls, calls}
  """
  use GenServer

  def start_link, do: GenServer.start_link(__MODULE__, nil)

  @doc "Returns the recorded calls (oldest first). Only valid while alive."
  def calls(pid), do: GenServer.call(pid, :calls)

  @doc "Registers a pid to receive `{:captured_calls, calls}` on terminate."
  def set_watcher(pid, watcher), do: GenServer.call(pid, {:set_watcher, watcher})

  @impl true
  def init(_), do: {:ok, {[], nil}}

  @impl true
  def handle_call(:calls, _from, {calls, watcher}),
    do: {:reply, Enum.reverse(calls), {calls, watcher}}

  def handle_call({:set_watcher, watcher}, _from, {calls, _}),
    do: {:reply, :ok, {calls, watcher}}

  def handle_call({:join_group, group, member_id, opts}, _from, {calls, watcher}) do
    resp = {:ok, %KafkaEx.Messages.JoinGroup{generation_id: 1, member_id: "m", leader_id: "x", members: []}}
    {:reply, resp, {[{:join_group, group, member_id, opts} | calls], watcher}}
  end

  def handle_call({:sync_group, group, gen, member_id, opts}, _from, {calls, watcher}) do
    resp = {:ok, %KafkaEx.Messages.SyncGroup{partition_assignments: []}}
    {:reply, resp, {[{:sync_group, group, gen, member_id, opts} | calls], watcher}}
  end

  def handle_call({:heartbeat, group, member_id, gen, opts}, _from, {calls, watcher}) do
    {:reply, {:ok, %KafkaEx.Messages.Heartbeat{}}, {[{:heartbeat, group, member_id, gen, opts} | calls], watcher}}
  end

  def handle_call({:leave_group, group, member_id, opts}, _from, {calls, watcher}) do
    {:reply, {:ok, %KafkaEx.Messages.LeaveGroup{}}, {[{:leave_group, group, member_id, opts} | calls], watcher}}
  end

  def handle_call({:create_topics, topics, timeout, opts}, _from, {calls, watcher}) do
    {:reply, {:ok, %{}}, {[{:create_topics, topics, timeout, opts} | calls], watcher}}
  end

  def handle_call({:delete_topics, topics, timeout, opts}, _from, {calls, watcher}) do
    {:reply, {:ok, %{}}, {[{:delete_topics, topics, timeout, opts} | calls], watcher}}
  end

  @impl true
  def terminate(_reason, {calls, watcher}) when is_pid(watcher) do
    send(watcher, {:captured_calls, Enum.reverse(calls)})
  end

  def terminate(_reason, _state), do: :ok
end
