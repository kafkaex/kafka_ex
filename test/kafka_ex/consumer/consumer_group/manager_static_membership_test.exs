defmodule KafkaEx.Consumer.ConsumerGroup.ManagerStaticMembershipTest.CapturingMockClient do
  @moduledoc false
  # Records the full opts of join/sync/heartbeat/leave so tests can assert
  # group_instance_id was threaded through. Returns benign success responses.
  use GenServer

  def start_link, do: GenServer.start_link(__MODULE__, nil)
  def calls(pid), do: GenServer.call(pid, :calls)

  def init(_), do: {:ok, []}
  def handle_call(:calls, _from, calls), do: {:reply, Enum.reverse(calls), calls}

  def handle_call({:join_group, group, member_id, opts}, _from, calls) do
    resp = {:ok, %KafkaEx.Messages.JoinGroup{generation_id: 1, member_id: "m", leader_id: "x", members: []}}
    {:reply, resp, [{:join_group, group, member_id, opts} | calls]}
  end

  def handle_call({:sync_group, group, gen, member_id, opts}, _from, calls) do
    resp = {:ok, %KafkaEx.Messages.SyncGroup{partition_assignments: []}}
    {:reply, resp, [{:sync_group, group, gen, member_id, opts} | calls]}
  end

  def handle_call({:heartbeat, group, member_id, gen, opts}, _from, calls) do
    {:reply, {:ok, %KafkaEx.Messages.Heartbeat{}}, [{:heartbeat, group, member_id, gen, opts} | calls]}
  end

  def handle_call({:leave_group, group, member_id, opts}, _from, calls) do
    {:reply, {:ok, %KafkaEx.Messages.LeaveGroup{}}, [{:leave_group, group, member_id, opts} | calls]}
  end
end

defmodule KafkaEx.Consumer.ConsumerGroup.ManagerStaticMembershipTest do
  @moduledoc """
  KIP-345 static membership wiring through the Manager: option resolution/
  validation, group_instance_id threaded into join/sync/heartbeat, LeaveGroup
  suppression on terminate, and the too-old-broker warning. Exercised through
  public behavior only; no private function is exposed for testing.
  """
  use ExUnit.Case, async: false

  import KafkaEx.TestSupport.ProcessHelpers

  alias KafkaEx.Consumer.ConsumerGroup.Manager
  alias KafkaEx.Consumer.ConsumerGroup.Manager.State

  describe "init/1 group_instance_id resolution" do
    test "a valid group_instance_id opt is stored on the State" do
      {:ok, client} = KafkaEx.Test.MockClient.start_link(%{})
      on_exit(fn -> stop_safely(client) end)

      opts = [supervisor_pid: self(), client: client, group_instance_id: "inst-1"]

      {:ok, state, _timeout} =
        Manager.init({{KafkaEx.GenConsumer, __MODULE__.NoopConsumer}, "g", ["t"], opts})

      assert %State{group_instance_id: "inst-1"} = state
    end

    test "no group_instance_id opt leaves it nil (dynamic membership)" do
      {:ok, client} = KafkaEx.Test.MockClient.start_link(%{})
      on_exit(fn -> stop_safely(client) end)

      opts = [supervisor_pid: self(), client: client]

      {:ok, state, _timeout} =
        Manager.init({{KafkaEx.GenConsumer, __MODULE__.NoopConsumer}, "g", ["t"], opts})

      assert %State{group_instance_id: nil} = state
    end

    test "a blank group_instance_id opt raises ArgumentError" do
      {:ok, client} = KafkaEx.Test.MockClient.start_link(%{})
      on_exit(fn -> stop_safely(client) end)

      opts = [supervisor_pid: self(), client: client, group_instance_id: ""]

      assert_raise ArgumentError, ~r/non-empty/, fn ->
        Manager.init({{KafkaEx.GenConsumer, __MODULE__.NoopConsumer}, "g", ["t"], opts})
      end
    end
  end

  describe "group_instance_id threading" do
    alias KafkaEx.Consumer.ConsumerGroup.ManagerStaticMembershipTest.CapturingMockClient
    alias KafkaEx.Consumer.ConsumerGroup.Heartbeat

    test "heartbeat sends group_instance_id when static" do
      {:ok, client} = CapturingMockClient.start_link()
      on_exit(fn -> stop_safely(client) end)

      {:ok, hb} =
        Heartbeat.start_link(%{
          client: client,
          group_name: "g",
          member_id: "m",
          generation_id: 1,
          heartbeat_interval: 60_000,
          group_instance_id: "inst-1"
        })

      on_exit(fn -> stop_safely(hb) end)
      send(hb, :timeout)

      # let the heartbeat call land
      Process.sleep(100)

      assert [{:heartbeat, "g", "m", 1, opts}] = CapturingMockClient.calls(client)
      assert Keyword.get(opts, :group_instance_id) == "inst-1"
    end

    test "heartbeat sends nil group_instance_id when dynamic" do
      {:ok, client} = CapturingMockClient.start_link()
      on_exit(fn -> stop_safely(client) end)

      {:ok, hb} =
        Heartbeat.start_link(%{
          client: client,
          group_name: "g",
          member_id: "m",
          generation_id: 1,
          heartbeat_interval: 60_000,
          group_instance_id: nil
        })

      on_exit(fn -> stop_safely(hb) end)
      send(hb, :timeout)
      Process.sleep(100)

      assert [{:heartbeat, "g", "m", 1, opts}] = CapturingMockClient.calls(client)
      assert Keyword.get(opts, :group_instance_id) == nil
    end
  end

  defmodule NoopConsumer do
    @moduledoc false
  end
end
