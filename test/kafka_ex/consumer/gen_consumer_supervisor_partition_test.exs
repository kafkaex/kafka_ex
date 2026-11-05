defmodule KafkaEx.Consumer.GenConsumer.SupervisorPartitionTest do
  @moduledoc """
  Per-partition control of `GenConsumer.Supervisor`: `start_partition/3`
  children are `restart: :temporary`, so the caller (the group manager) is the
  only one that starts, stops or replaces a partition's consumer.
  """
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog
  import KafkaEx.TestSupport.ProcessHelpers

  alias KafkaEx.Consumer.GenConsumer.Supervisor, as: GenConsumerSupervisor

  # Reports its start args and its terminate/2 to the test process, so a test
  # can see which opts a partition got and that a stop ran the consumer's
  # shutdown path (where GenConsumer commits).
  defmodule ReportingGenConsumer do
    use GenServer

    def start_link(consumer_module, group_name, topic, partition, opts) do
      case opts[:start_result] do
        nil -> GenServer.start_link(__MODULE__, {consumer_module, group_name, topic, partition, opts})
        result -> result
      end
    end

    def init({_consumer_module, _group_name, _topic, _partition, opts} = args) do
      Process.flag(:trap_exit, true)
      if test_pid = opts[:test_pid], do: send(test_pid, {:started, self(), args})
      {:ok, args}
    end

    def handle_call(:crash, _from, state), do: {:stop, :crash, :ok, state}

    # Busy until told to go on (stands in for a fetch long-poll).
    def handle_cast({:block, test_pid, release_after}, state) do
      send(test_pid, {:blocked, self()})

      receive do
        :unblock -> {:noreply, state}
      after
        release_after -> {:noreply, state}
      end
    end

    # Busy until a stop request arrives, then for `delay` ms more before
    # handling it: a fetch long-poll that is still running when the stop
    # comes in, so serial stops of N such consumers take N * delay.
    def handle_cast({:block_until_stop, test_pid, delay}, state) do
      send(test_pid, {:blocked, self()})

      receive do
        {:system, _from, {:terminate, _reason}} = stop ->
          Process.sleep(delay)
          send(self(), stop)
          {:noreply, state}
      end
    end

    def terminate(reason, {_, _, topic, partition, opts}) do
      if test_pid = opts[:test_pid], do: send(test_pid, {:terminated, self(), {topic, partition}, reason})
      :ok
    end
  end

  defmodule MockConsumer do
  end

  defp start_sup(assignments \\ [], opts \\ []) do
    {:ok, sup} =
      GenConsumerSupervisor.start_link(%{
        gen_consumer_module: ReportingGenConsumer,
        consumer_module: MockConsumer,
        group_name: "test-group",
        assignments: assignments,
        opts: Keyword.put(opts, :test_pid, self())
      })

    on_exit(fn -> stop_safely(sup) end)
    sup
  end

  # DynamicSupervisor keeps {mfa, restart, shutdown, type, modules} per child.
  defp child_info(sup, pid) do
    %{children: children} = :sys.get_state(sup)
    Map.fetch!(children, pid)
  end

  # Overwrites the :shutdown the supervisor indexed for a partition.
  defp put_shutdown(sup, tp, shutdown) do
    key = {GenConsumerSupervisor, :partition}

    :sys.replace_state(sup, fn state ->
      {pid, _} = Process.get({key, tp})
      Process.put({key, tp}, {pid, shutdown})
      state
    end)
  end

  # Also consumes the pid's {:started} report, so a later refute_receive of
  # {:started, ...} can only match a restart.
  defp block(pid, release_after \\ :infinity) do
    assert_receive {:started, ^pid, _}
    GenServer.cast(pid, {:block, self(), release_after})
    assert_receive {:blocked, ^pid}
  end

  describe "start_partition/3" do
    test "starts a consumer with the supervisor's args, merged with the given opts" do
      sup = start_sup([], commit_interval: 1_000, member_id: "old")
      assert {:ok, pid} = GenConsumerSupervisor.start_partition(sup, {"t", 3}, member_id: "m", member_epoch: 5)

      assert_receive {:started, ^pid, {MockConsumer, "test-group", "t", 3, opts}}
      assert opts[:member_id] == "m"
      assert opts[:member_epoch] == 5
      assert opts[:commit_interval] == 1_000

      assert GenConsumerSupervisor.lookup_partition(sup, {"t", 3}) == {:ok, pid}
      assert GenConsumerSupervisor.partition_pids(sup) == %{{"t", 3} => pid}
      assert GenConsumerSupervisor.child_pids(sup) == [pid]
    end

    test "a custom gen_consumer_module gets exactly the merged opts, with no internal keys" do
      sup = start_sup([{"t", 0}], foo: 1)
      assert {:ok, pid} = GenConsumerSupervisor.start_partition(sup, {"t", 1}, member_epoch: 2)

      assert_receive {:started, _, {MockConsumer, "test-group", "t", 0, opts0}}
      assert_receive {:started, ^pid, {MockConsumer, "test-group", "t", 1, opts1}}
      assert Enum.sort(opts0) == Enum.sort(foo: 1, test_pid: self())
      assert Enum.sort(opts1) == Enum.sort(foo: 1, member_epoch: 2, test_pid: self())
    end

    test "children are :temporary and report the consumer module" do
      sup = start_sup()
      {:ok, pid} = GenConsumerSupervisor.start_partition(sup, {"t", 1})

      assert {_mfa, :temporary, _shutdown, :worker, [ReportingGenConsumer]} = child_info(sup, pid)
      assert [{:undefined, ^pid, :worker, [ReportingGenConsumer]}] = DynamicSupervisor.which_children(sup)
    end

    test "a partition that is already running is not started twice" do
      sup = start_sup()
      {:ok, pid} = GenConsumerSupervisor.start_partition(sup, {"t", 0})

      assert GenConsumerSupervisor.start_partition(sup, {"t", 0}) == {:error, {:already_started, pid}}
      assert GenConsumerSupervisor.child_pids(sup) == [pid]
    end

    test "concurrent starts of one partition start exactly one consumer" do
      sup = start_sup()

      results =
        1..20
        |> Enum.map(fn _ -> Task.async(fn -> GenConsumerSupervisor.start_partition(sup, {"t", 0}) end) end)
        |> Enum.map(&Task.await/1)

      assert [{:ok, pid}] = Enum.filter(results, &match?({:ok, _}, &1))
      assert Enum.count(results, &(&1 == {:error, {:already_started, pid}})) == 19
      assert GenConsumerSupervisor.child_pids(sup) == [pid]
    end

    test "a partition can be started again after it was stopped" do
      sup = start_sup()
      {:ok, pid} = GenConsumerSupervisor.start_partition(sup, {"t", 0})
      assert :ok = GenConsumerSupervisor.stop_partition(sup, {"t", 0})
      assert {:ok, new_pid} = GenConsumerSupervisor.start_partition(sup, {"t", 0})
      assert new_pid != pid
      assert GenConsumerSupervisor.lookup_partition(sup, {"t", 0}) == {:ok, new_pid}
    end

    test "a partition whose consumer crashed can be started again by the caller" do
      sup = start_sup()
      {:ok, pid} = GenConsumerSupervisor.start_partition(sup, {"t", 0})
      ref = Process.monitor(pid)

      capture_log(fn ->
        :ok = GenServer.call(pid, :crash)
        assert_receive {:DOWN, ^ref, :process, ^pid, :crash}
      end)

      assert GenConsumerSupervisor.lookup_partition(sup, {"t", 0}) == :error
      assert {:ok, new_pid} = GenConsumerSupervisor.start_partition(sup, {"t", 0}, member_epoch: 9)
      assert_receive {:started, ^new_pid, {_, _, "t", 0, opts}}
      assert opts[:member_epoch] == 9
    end

    test "a partition given to start_link/1 is refused" do
      sup = start_sup([{"t", 0}])
      [pid] = GenConsumerSupervisor.child_pids(sup)

      assert GenConsumerSupervisor.start_partition(sup, {"t", 0}) == {:error, :static_partition}
      assert GenConsumerSupervisor.child_pids(sup) == [pid]
    end

    test "returns the consumer's start error and indexes nothing" do
      sup = start_sup([], start_result: {:error, :boom})
      assert GenConsumerSupervisor.start_partition(sup, {"t", 0}) == {:error, :boom}
      assert GenConsumerSupervisor.lookup_partition(sup, {"t", 0}) == :error
      assert GenConsumerSupervisor.child_pids(sup) == []
    end

    test "malformed input is an error tuple, not an exception" do
      sup = start_sup()

      for bad <- [{"t", -1}, {"t", 2_147_483_648}, {:t, 0}, {"t", 1.0}, "t", nil] do
        assert GenConsumerSupervisor.start_partition(sup, bad) == {:error, :invalid_topic_partition}
        assert GenConsumerSupervisor.lookup_partition(sup, bad) == :error
      end

      assert GenConsumerSupervisor.start_partition(sup, {"t", 0}, %{}) == {:error, :invalid_options}
      assert DynamicSupervisor.which_children(sup) == []
      refute_received {:started, _, _}
    end

    test "opts that are not a keyword list, or a malformed :fetch_options, are {:error, :invalid_options}" do
      sup = start_sup()

      for bad <- [[1, 2], [{"a", 1}], [:foo], [fetch_options: :bogus], [fetch_options: nil], [fetch_options: [1]]] do
        assert GenConsumerSupervisor.start_partition(sup, {"t", 0}, bad) == {:error, :invalid_options}
      end

      assert DynamicSupervisor.which_children(sup) == []
      refute_received {:started, _, _}
      assert {:ok, _} = GenConsumerSupervisor.start_partition(sup, {"t", 0}, fetch_options: [max_wait_time: 1])
    end

    test "a malformed :fetch_options in the supervisor opts is {:error, :invalid_options}" do
      sup = start_sup([], fetch_options: :bogus)
      assert GenConsumerSupervisor.start_partition(sup, {"t", 0}) == {:error, :invalid_options}
      assert DynamicSupervisor.which_children(sup) == []
    end

    test "returns {:error, :noproc} for a supervisor that is not running" do
      sup = start_sup()
      stop_safely(sup)
      assert GenConsumerSupervisor.start_partition(sup, {"t", 0}) == {:error, :noproc}
    end

    test "inherits the supervisor's :shutdown unless opts override it" do
      sup = start_sup([], shutdown: 1_234)
      {:ok, pid1} = GenConsumerSupervisor.start_partition(sup, {"t", 1})
      {:ok, pid2} = GenConsumerSupervisor.start_partition(sup, {"t", 2}, shutdown: 777)

      assert {_, _, 1_234, _, _} = child_info(sup, pid1)
      assert {_, _, 777, _, _} = child_info(sup, pid2)
    end

    test "without a :shutdown, leaves room for a fetch long-poll plus the final commit" do
      sup = start_sup()
      {:ok, pid1} = GenConsumerSupervisor.start_partition(sup, {"t", 1})
      {:ok, pid2} = GenConsumerSupervisor.start_partition(sup, {"t", 2}, fetch_options: [max_wait_time: 30_000])

      assert {_, _, 20_000, _, _} = child_info(sup, pid1)
      assert {_, _, 40_000, _, _} = child_info(sup, pid2)
    end
  end

  describe "no automatic restarts" do
    for reason <- [:crash, :killed] do
      test "a consumer that exits abnormally (#{reason}) is not restarted; the caller's monitor sees it" do
        sup = start_sup()
        {:ok, pid} = GenConsumerSupervisor.start_partition(sup, {"t", 0})
        assert_receive {:started, ^pid, _}
        ref = Process.monitor(pid)

        capture_log(fn ->
          case unquote(reason) do
            :crash -> :ok = GenServer.call(pid, :crash)
            :killed -> Process.exit(pid, :kill)
          end

          assert_receive {:DOWN, ^ref, :process, ^pid, unquote(reason)}
          # Ordered after the supervisor handled the EXIT.
          _ = DynamicSupervisor.count_children(sup)
        end)

        refute_receive {:started, _, _}, 100
        assert DynamicSupervisor.which_children(sup) == []
        assert GenConsumerSupervisor.partition_pids(sup) == %{}
        assert :sys.get_state(sup).restarts == []
        assert Process.alive?(sup)
      end
    end

    test "more crashes than the restart intensity do not take the supervisor down" do
      sup = start_sup()
      sup_ref = Process.monitor(sup)

      capture_log(fn ->
        for p <- 0..9 do
          {:ok, pid} = GenConsumerSupervisor.start_partition(sup, {"t", p})
          ref = Process.monitor(pid)
          Process.exit(pid, :kill)
          assert_receive {:DOWN, ^ref, :process, ^pid, :killed}
        end

        _ = DynamicSupervisor.count_children(sup)
      end)

      refute_received {:DOWN, ^sup_ref, _, _, _}
      assert DynamicSupervisor.which_children(sup) == []
    end

    test "classic start_link/1 children are still :transient and restarted" do
      sup = start_sup([{"t", 0}])
      [pid] = GenConsumerSupervisor.child_pids(sup)
      assert {_mfa, :transient, 5_000, :worker, _} = child_info(sup, pid)

      capture_log(fn ->
        :ok = GenServer.call(pid, :crash)
        assert_receive {:started, new_pid, {_, _, "t", 0, _}} when new_pid != pid
      end)
    end
  end

  describe "stop_partition/2" do
    test "stops only that partition, through the consumer's terminate/2" do
      sup = start_sup()
      {:ok, pid0} = GenConsumerSupervisor.start_partition(sup, {"t", 0})
      {:ok, pid1} = GenConsumerSupervisor.start_partition(sup, {"t", 1})

      assert :ok = GenConsumerSupervisor.stop_partition(sup, {"t", 0})

      assert_receive {:terminated, ^pid0, {"t", 0}, :shutdown}
      refute Process.alive?(pid0)
      assert Process.alive?(pid1)
      assert GenConsumerSupervisor.lookup_partition(sup, {"t", 0}) == :error
      _ = DynamicSupervisor.count_children(sup)
      assert GenConsumerSupervisor.child_pids(sup) == [pid1]
    end

    test "lost: true stops the consumer with {:shutdown, :partition_lost}" do
      sup = start_sup()
      {:ok, pid0} = GenConsumerSupervisor.start_partition(sup, {"t", 0})
      {:ok, pid1} = GenConsumerSupervisor.start_partition(sup, {"t", 1})

      assert :ok = GenConsumerSupervisor.stop_partition(sup, {"t", 0}, lost: true)
      assert_receive {:terminated, ^pid0, {"t", 0}, {:shutdown, :partition_lost}}

      assert %{{"t", 1} => :ok} = GenConsumerSupervisor.stop_partitions(sup, [{"t", 1}], lost: true)
      assert_receive {:terminated, ^pid1, {"t", 1}, {:shutdown, :partition_lost}}
    end

    test "an unknown or already stopped partition is {:error, :not_found}" do
      sup = start_sup([{"s", 0}])
      assert GenConsumerSupervisor.stop_partition(sup, {"t", 9}) == {:error, :not_found}
      # Classic partitions are not the caller's to stop one by one.
      assert GenConsumerSupervisor.stop_partition(sup, {"s", 0}) == {:error, :not_found}

      {:ok, _} = GenConsumerSupervisor.start_partition(sup, {"t", 0})
      assert :ok = GenConsumerSupervisor.stop_partition(sup, {"t", 0})
      assert GenConsumerSupervisor.stop_partition(sup, {"t", 0}) == {:error, :not_found}
    end

    test "a consumer stuck past its :shutdown is killed and not restarted" do
      sup = start_sup([], shutdown: 100)
      {:ok, pid} = GenConsumerSupervisor.start_partition(sup, {"t", 0})
      block(pid)
      ref = Process.monitor(pid)

      {elapsed_us, result} = :timer.tc(fn -> GenConsumerSupervisor.stop_partition(sup, {"t", 0}) end)

      assert result == :ok
      assert_receive {:DOWN, ^ref, :process, ^pid, :killed}
      assert elapsed_us < 2_000_000
      _ = DynamicSupervisor.count_children(sup)
      refute_receive {:started, _, {_, _, "t", 0, _}}, 100
      assert :sys.get_state(sup).restarts == []
      assert DynamicSupervisor.which_children(sup) == []
      assert Process.alive?(sup)
    end

    test "returns only once a killed consumer is dead, so the partition can be started again at once" do
      sup = start_sup([], shutdown: 50)

      capture_log(fn ->
        for _ <- 1..20 do
          {:ok, pid} = GenConsumerSupervisor.start_partition(sup, {"t", 0})
          block(pid)

          assert :ok = GenConsumerSupervisor.stop_partition(sup, {"t", 0})
          # No wait of any kind between the stop and these checks.
          refute Process.alive?(pid)
          assert GenConsumerSupervisor.lookup_partition(sup, {"t", 0}) == :error
          assert GenConsumerSupervisor.partition_pids(sup) == %{}
          assert {:ok, new_pid} = GenConsumerSupervisor.start_partition(sup, {"t", 0})
          assert new_pid != pid
          assert :ok = GenConsumerSupervisor.stop_partition(sup, {"t", 0})
          refute Process.alive?(new_pid)
        end
      end)

      # stop_partition/2 consumed the :DOWN of its own monitor before
      # returning (the wait for the kill): nothing leaks into the caller's
      # mailbox, where an owner monitoring its consumers would see it.
      refute_receive {:DOWN, _, :process, _, _}, 200
    end

    test "with shutdown: :brutal_kill the consumer is killed, not restarted" do
      sup = start_sup([], shutdown: :brutal_kill)
      {:ok, pid} = GenConsumerSupervisor.start_partition(sup, {"t", 0})
      assert_receive {:started, ^pid, _}
      ref = Process.monitor(pid)

      {result, _log} = with_log(fn -> GenConsumerSupervisor.stop_partition(sup, {"t", 0}) end)

      assert result == :ok
      refute Process.alive?(pid)
      assert_receive {:DOWN, ^ref, :process, ^pid, :killed}
      # Killed: terminate/2 never ran.
      refute_received {:terminated, ^pid, _, _}
      _ = DynamicSupervisor.count_children(sup)
      refute_receive {:started, _, {_, _, "t", 0, _}}, 100
      assert DynamicSupervisor.which_children(sup) == []
    end

    test "a stop request that fails for another reason still leaves the consumer dead" do
      # A non-integer, non-:infinity :shutdown makes GenServer.stop/3 exit with
      # something other than a timeout while the consumer is still running.
      sup = start_sup()
      {:ok, pid} = GenConsumerSupervisor.start_partition(sup, {"t", 0}, shutdown: 100)
      assert_receive {:started, ^pid, _}
      put_shutdown(sup, {"t", 0}, :bogus)

      {result, _log} = with_log(fn -> GenConsumerSupervisor.stop_partition(sup, {"t", 0}) end)

      assert result == :ok
      refute Process.alive?(pid)
      assert GenConsumerSupervisor.lookup_partition(sup, {"t", 0}) == :error
    end

    test "a malformed partition is {:error, :invalid_topic_partition}" do
      sup = start_sup()

      for bad <- [{"t", -1}, {"t", 2_147_483_648}, {:t, 0}, {"t", "0"}, "t", nil] do
        assert GenConsumerSupervisor.stop_partition(sup, bad) == {:error, :invalid_topic_partition}
      end
    end

    test "a consumer that crashes while being stopped is not restarted" do
      sup = start_sup()
      {:ok, pid} = GenConsumerSupervisor.start_partition(sup, {"t", 0})
      block(pid)
      task = Task.async(fn -> GenConsumerSupervisor.stop_partition(sup, {"t", 0}) end)
      wait_until(fn -> match?({:messages, [_ | _]}, Process.info(pid, :messages)) end)

      capture_log(fn ->
        Process.exit(pid, :kill)
        assert Task.await(task) == :ok
        _ = DynamicSupervisor.count_children(sup)
      end)

      refute_receive {:started, _, {_, _, "t", 0, _}}, 100
      assert DynamicSupervisor.which_children(sup) == []
      assert {:ok, _} = GenConsumerSupervisor.start_partition(sup, {"t", 0})
    end

    test "a supervisor serves other partitions while a stop waits on a busy consumer" do
      sup = start_sup()
      {:ok, pid0} = GenConsumerSupervisor.start_partition(sup, {"t", 0})
      block(pid0)
      task = Task.async(fn -> GenConsumerSupervisor.stop_partition(sup, {"t", 0}) end)
      wait_until(fn -> match?({:messages, [_ | _]}, Process.info(pid0, :messages)) end)

      assert {:ok, pid1} = GenConsumerSupervisor.start_partition(sup, {"t", 1})
      assert :ok = GenConsumerSupervisor.stop_partition(sup, {"t", 1})
      assert Process.alive?(pid0)

      send(pid0, :unblock)
      assert Task.await(task) == :ok
      assert_receive {:terminated, ^pid1, {"t", 1}, :shutdown}
      assert_receive {:terminated, ^pid0, {"t", 0}, :shutdown}
    end
  end

  describe "stop_partitions/2" do
    test "stops many long-polling consumers concurrently, each through terminate/2" do
      sup = start_sup()
      tps = for p <- 0..7, do: {"t", p}
      pids = for tp <- tps, do: elem(GenConsumerSupervisor.start_partition(sup, tp), 1)
      # Each consumer handles a stop 300 ms after it arrives: serial stops
      # would take >= 8 * 300 ms.
      for pid <- pids do
        assert_receive {:started, ^pid, _}
        GenServer.cast(pid, {:block_until_stop, self(), 300})
        assert_receive {:blocked, ^pid}
      end

      {elapsed_us, result} = :timer.tc(fn -> GenConsumerSupervisor.stop_partitions(sup, tps ++ [{"t", 99}]) end)

      assert result == Map.put(Map.new(tps, &{&1, :ok}), {"t", 99}, {:error, :not_found})
      assert elapsed_us < 1_500_000

      for {pid, tp} <- Enum.zip(pids, tps) do
        assert_receive {:terminated, ^pid, ^tp, :shutdown}
      end

      _ = DynamicSupervisor.count_children(sup)
      assert DynamicSupervisor.which_children(sup) == []
    end

    test "kills every consumer stuck past its :shutdown without restarting any" do
      tps = for p <- 0..5, do: {"t", p}
      sup = start_sup([], shutdown: 100)
      pids = for tp <- tps, do: elem(GenConsumerSupervisor.start_partition(sup, tp), 1)
      for pid <- pids, do: block(pid)
      sup_ref = Process.monitor(sup)

      {result, _log} = with_log(fn -> GenConsumerSupervisor.stop_partitions(sup, tps) end)

      assert result == Map.new(tps, &{&1, :ok})
      refute Enum.any?(pids, &Process.alive?/1)
      _ = DynamicSupervisor.count_children(sup)
      refute_received {:DOWN, ^sup_ref, _, _, _}
      refute_receive {:started, _, _}, 100
      assert :sys.get_state(sup).restarts == []
      assert DynamicSupervisor.which_children(sup) == []
    end
  end

  describe "stop_partitions/2 and the caller" do
    test "a caller that traps exits gets no :EXIT messages from the stops" do
      sup = start_sup()
      tps = for p <- 0..3, do: {"t", p}
      for tp <- tps, do: {:ok, _} = GenConsumerSupervisor.start_partition(sup, tp)

      Process.flag(:trap_exit, true)
      assert GenConsumerSupervisor.stop_partitions(sup, tps) == Map.new(tps, &{&1, :ok})
      refute_receive {:EXIT, _, _}, 100
    end

    test "malformed input is {:error, :invalid_topic_partition}, not an exception" do
      sup = start_sup()
      assert GenConsumerSupervisor.stop_partitions(sup, :all) == {:error, :invalid_topic_partition}

      assert GenConsumerSupervisor.stop_partitions(sup, [{"t", -1}, {"t", 0}]) == %{
               {"t", -1} => {:error, :invalid_topic_partition},
               {"t", 0} => {:error, :not_found}
             }
    end
  end

  describe "lookup_partition/2 and partition_pids/1" do
    test "only list live start_partition/3 consumers" do
      sup = start_sup([{"s", 0}])
      {:ok, pid0} = GenConsumerSupervisor.start_partition(sup, {"t", 0})
      {:ok, pid1} = GenConsumerSupervisor.start_partition(sup, {"t", 1})
      ref = Process.monitor(pid1)

      capture_log(fn ->
        Process.exit(pid1, :kill)
        assert_receive {:DOWN, ^ref, :process, ^pid1, :killed}
      end)

      assert GenConsumerSupervisor.partition_pids(sup) == %{{"t", 0} => pid0}
      assert GenConsumerSupervisor.lookup_partition(sup, {"t", 1}) == :error
      assert GenConsumerSupervisor.lookup_partition(sup, {"s", 0}) == :error
    end

    test "do not block on a busy supervisor" do
      sup = start_sup()
      {:ok, pid} = GenConsumerSupervisor.start_partition(sup, {"t", 0})
      :ok = :sys.suspend(sup)

      try do
        assert GenConsumerSupervisor.lookup_partition(sup, {"t", 0}) == {:ok, pid}
        assert GenConsumerSupervisor.partition_pids(sup) == %{{"t", 0} => pid}
      after
        :sys.resume(sup)
      end
    end

    test "are :error / %{} for a supervisor that is not running" do
      sup = start_sup()
      {:ok, _} = GenConsumerSupervisor.start_partition(sup, {"t", 0})
      stop_safely(sup)
      assert GenConsumerSupervisor.lookup_partition(sup, {"t", 0}) == :error
      assert GenConsumerSupervisor.partition_pids(sup) == %{}
      assert GenConsumerSupervisor.stop_partition(sup, {"t", 0}) == {:error, :not_found}
    end

    test "the index keeps no entries for dead consumers" do
      sup = start_sup()

      for p <- 0..9 do
        {:ok, _} = GenConsumerSupervisor.start_partition(sup, {"t", p})
        :ok = GenConsumerSupervisor.stop_partition(sup, {"t", p})
      end

      {:ok, _} = GenConsumerSupervisor.start_partition(sup, {"u", 0})
      {:dictionary, dict} = Process.info(sup, :dictionary)

      assert [{"u", 0}] ==
               for({{GenConsumerSupervisor, :partition}, tp} <- Enum.map(dict, &elem(&1, 0)), do: tp)
    end
  end

  defp wait_until(fun, attempts \\ 200) do
    cond do
      fun.() -> :ok
      attempts == 0 -> flunk("condition not met")
      true -> Process.sleep(5) && wait_until(fun, attempts - 1)
    end
  end
end
