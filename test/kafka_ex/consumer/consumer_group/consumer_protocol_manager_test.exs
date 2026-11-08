defmodule KafkaEx.Consumer.ConsumerGroup.ConsumerProtocolManagerTest do
  @moduledoc """
  The KIP-848 member state machine, run under a real
  `KafkaEx.Consumer.ConsumerGroup` supervisor.

  The client is scripted: every ConsumerGroupHeartbeat and Metadata call is
  forwarded to the test process, which answers it, so each protocol step is
  explicit. Per-partition consumers are `FakeConsumer`s, started through the
  real `GenConsumer.Supervisor.start_partition/3`, that report their start
  options, membership updates and `terminate/2` reason.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias KafkaEx.Client.RequestBuilder
  alias KafkaEx.Client.State, as: ClientState
  alias KafkaEx.Cluster.ClusterMetadata
  alias KafkaEx.Cluster.Topic
  alias KafkaEx.Consumer.ConsumerGroup
  alias KafkaEx.Consumer.ConsumerGroup.ConsumerProtocolManager
  alias KafkaEx.Consumer.GenConsumer.Supervisor, as: GenConsumerSupervisor
  alias KafkaEx.Messages.ConsumerGroupHeartbeat
  alias KafkaEx.Messages.TopicPartitions

  @group "kip848-group"
  @id_a <<1::128>>
  @id_b <<2::128>>
  @long_interval 600_000

  defmodule ScriptedClient do
    @moduledoc false
    use GenServer

    def start_link(test_pid), do: GenServer.start_link(__MODULE__, test_pid)
    def init(test_pid), do: {:ok, test_pid}

    def handle_call({:consumer_group_heartbeat, group, opts}, from, test_pid) do
      send(test_pid, {:heartbeat_call, from, group, opts})
      {:noreply, test_pid}
    end

    def handle_call({:metadata, _topics, opts, _version}, from, test_pid) do
      send(test_pid, {:metadata_call, from, opts})
      {:noreply, test_pid}
    end

    def handle_call(other, _from, test_pid), do: {:reply, {:error, {:unexpected, other}}, test_pid}
  end

  defmodule FakeConsumer do
    @moduledoc false
    use GenServer

    def start_link(_consumer_module, _group, topic, partition, opts),
      do: GenServer.start_link(__MODULE__, {topic, partition, opts})

    # block_init/2 makes the next init of a partition wait for :release_init
    # (a slow start: in production the consumer's own client connects to
    # every bootstrap broker).
    def init({topic, partition, opts}) do
      Process.flag(:trap_exit, true)
      test_pid = Keyword.fetch!(opts, :test_pid)
      tp = {topic, partition}

      # fail_init/2: every init of the partition fails (a consumer that
      # cannot start, e.g. its client cannot reach the brokers).
      if :persistent_term.get({__MODULE__, :fail_init, test_pid, tp}, false) do
        send(test_pid, {:init_failed, tp})
        exit(:boom)
      end

      if :persistent_term.get({__MODULE__, :block_init, test_pid, tp}, false) do
        :persistent_term.erase({__MODULE__, :block_init, test_pid, tp})
        send(test_pid, {:init_blocked, self(), tp})

        receive do
          :release_init -> :ok
        end
      end

      send(test_pid, {:started, self(), tp, opts})
      {:ok, %{tp: tp, test: test_pid, block_stop: false, block_update: false, fail_update: 0}}
    end

    def block_init(test_pid, tp), do: :persistent_term.put({__MODULE__, :block_init, test_pid, tp}, true)
    def fail_init(test_pid, tp), do: :persistent_term.put({__MODULE__, :fail_init, test_pid, tp}, true)

    def handle_call(:block_stop, _from, state), do: {:reply, :ok, %{state | block_stop: true}}
    def handle_call(:block_update, _from, state), do: {:reply, :ok, %{state | block_update: true}}
    def handle_call({:fail_update, n}, _from, state), do: {:reply, :ok, %{state | fail_update: n}}

    def handle_call({:update_membership, _membership}, _from, %{fail_update: n} = state) when n > 0 do
      send(state.test, {:update_failed, self(), state.tp})
      {:reply, {:error, :boom}, %{state | fail_update: n - 1}}
    end

    def handle_call({:update_membership, membership}, _from, state) do
      if state.block_update do
        send(state.test, {:update_blocked, self(), state.tp})

        receive do
          :release_update -> :ok
        end
      end

      send(state.test, {:membership, self(), state.tp, membership})
      {:reply, :ok, state}
    end

    def handle_call({:exit, reason}, _from, state), do: {:stop, reason, :ok, state}

    def terminate(reason, state) do
      if state.block_stop do
        send(state.test, {:stop_blocked, self(), state.tp})

        receive do
          :release_stop -> :ok
        end
      end

      send(state.test, {:terminated, self(), state.tp, reason})
      :ok
    end
  end

  defmodule NoopConsumer do
    @moduledoc false
  end

  # --- helpers ----------------------------------------------------------------

  defp start_group(opts \\ [], topics \\ ["a"]) do
    {:ok, client} = ScriptedClient.start_link(self())
    Process.unlink(client)

    opts =
      Keyword.merge(
        [
          client: client,
          test_pid: self(),
          __consumer_protocol_manager__: true,
          heartbeat_retry_backoff_ms: 10,
          heartbeat_retry_max_backoff_ms: 20,
          partition_restart_backoff_ms: 10,
          partition_restart_max_backoff_ms: 20,
          topic_resolution_backoff_ms: 10,
          topic_resolution_max_backoff_ms: 20,
          leave_timeout_ms: 1_000
        ],
        opts
      )

    {:ok, sup} = ConsumerGroup.start_link({FakeConsumer, NoopConsumer}, @group, topics, opts)
    Process.unlink(sup)
    manager = ConsumerGroup.get_manager_pid(sup)

    test_pid = self()

    on_exit(fn ->
      for {{FakeConsumer, kind, ^test_pid, _} = key, _} <- :persistent_term.get(),
          kind in [:block_init, :fail_init],
          do: :persistent_term.erase(key)

      # Supervisor.stop/1: a supervisor ignores exit signals from processes
      # other than its parent.
      try do
        Supervisor.stop(sup, :shutdown, 5_000)
      catch
        :exit, _ -> Process.exit(sup, :kill)
      end

      Process.exit(client, :kill)
    end)

    %{sup: sup, client: client, manager: manager}
  end

  defp next_heartbeat(timeout \\ 2_000) do
    assert_receive {:heartbeat_call, from, @group, opts}, timeout
    {from, opts}
  end

  defp reply_ok(from, opts, epoch, assignment \\ nil, interval \\ @long_interval) do
    GenServer.reply(
      from,
      {:ok,
       %ConsumerGroupHeartbeat{
         member_id: opts[:member_id],
         member_epoch: epoch,
         heartbeat_interval_ms: interval,
         assignment: assignment
       }}
    )
  end

  defp assignment(spec) do
    for {id, name, partitions} <- spec, do: %TopicPartitions{topic_id: id, topic_name: name, partitions: partitions}
  end

  defp owned_partitions(opts) do
    case Keyword.fetch(opts, :topic_partitions) do
      {:ok, tps} ->
        tps |> Enum.flat_map(fn %{topic_id: id, partitions: ps} -> Enum.map(ps, &{id, &1}) end) |> Enum.sort()

      :error ->
        :unchanged
    end
  end

  # Asks the manager for an immediate heartbeat (what a consumer's
  # :rejoin_required cast does) and returns it.
  defp poke(manager) do
    GenServer.cast(manager, {:rejoin_required, :test_poke, nil})
    next_heartbeat()
  end

  # Join, get `parts` of topic "a" at `epoch` and acknowledge them.
  defp join_with(parts, epoch \\ 1) do
    {from, opts} = next_heartbeat()
    assert opts[:member_epoch] == 0
    reply_ok(from, opts, epoch, assignment([{@id_a, "a", parts}]))

    pids =
      Map.new(parts, fn p ->
        assert_receive {:started, pid, {"a", ^p}, start_opts}
        assert start_opts[:member_epoch] == epoch
        assert start_opts[:member_id] == opts[:member_id]
        {p, pid}
      end)

    {from, ack} = next_heartbeat()
    assert ack[:member_epoch] == epoch
    assert owned_partitions(ack) == Enum.map(parts, &{@id_a, &1})
    reply_ok(from, ack, epoch)
    {opts[:member_id], pids}
  end

  defp attach_telemetry(events) do
    test_pid = self()
    handler = "cpm-test-#{System.unique_integer()}"

    :telemetry.attach_many(
      handler,
      events,
      fn event, measurements, metadata, _ -> send(test_pid, {:telemetry, event, measurements, metadata}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)
  end

  defp stop_async(sup) do
    test_pid = self()

    spawn(fn ->
      ref = Process.monitor(sup)
      Supervisor.stop(sup, :shutdown, :infinity)

      receive do
        {:DOWN, ^ref, _, _, reason} -> send(test_pid, {:sup_down, reason})
      end
    end)
  end

  defp quiet(fun), do: capture_log(fun)

  # Waits (bounded) until the manager is blocked in a receive inside its
  # terminate/2, i.e. its shutdown has got as far as it can without help.
  defp await_blocked_in_terminate(manager, attempts \\ 200) do
    info = Process.info(manager, [:status, :current_stacktrace])

    in_terminate? =
      is_list(info) and info[:status] == :waiting and
        Enum.any?(info[:current_stacktrace], &match?({ConsumerProtocolManager, :terminate, 2, _}, &1))

    cond do
      in_terminate? -> :ok
      attempts == 0 -> flunk("manager never blocked in terminate/2: #{inspect(info)}")
      true -> Process.sleep(5) && await_blocked_in_terminate(manager, attempts - 1)
    end
  end

  # Waits (bounded) until `fun.(manager_state)` is truthy.
  defp await_state(manager, fun, attempts \\ 200) do
    cond do
      fun.(:sys.get_state(manager)) -> :ok
      attempts == 0 -> flunk("manager state never matched")
      true -> Process.sleep(5) && await_state(manager, fun, attempts - 1)
    end
  end

  # The first of a consumer's terminate or a heartbeat call, whichever is
  # first in the mailbox.
  defp next_terminated_or_heartbeat(pid) do
    receive do
      {:terminated, ^pid, tp, reason} -> {:terminated, tp, reason}
      {:heartbeat_call, from, @group, opts} -> {:heartbeat, from, opts}
    after
      2_000 -> flunk("neither a terminate nor a heartbeat")
    end
  end

  # Answers heartbeats until the leave (a negative epoch); returns it.
  defp drain_until_leave(attempts \\ 20) do
    {from, hb} = next_heartbeat()

    cond do
      hb[:member_epoch] < 0 -> {from, hb}
      attempts == 0 -> flunk("no leave")
      true -> reply_ok(from, hb, hb[:member_epoch], nil, 100) && drain_until_leave(attempts - 1)
    end
  end

  # --- join -------------------------------------------------------------------

  describe "join" do
    test "joins with a generated member id, epoch 0, the subscription and no owned partitions" do
      start_group(rebalance_timeout_ms: 45_000)
      {_from, opts} = next_heartbeat()

      assert opts[:member_epoch] == 0
      assert is_binary(opts[:member_id]) and byte_size(opts[:member_id]) == 22
      assert opts[:subscribed_topic_names] == ["a"]
      assert opts[:rebalance_timeout_ms] == 45_000
      assert opts[:topic_partitions] == []
      refute Keyword.has_key?(opts, :server_assignor)
      refute Keyword.has_key?(opts, :instance_id)
      refute Keyword.has_key?(opts, :subscribed_topic_regex)
    end

    test "passes server_assignor, instance_id and a regex subscription through" do
      start_group([server_assignor: "range", group_instance_id: "inst-1", subscribed_topic_regex: "a.*"], [])
      {_from, opts} = next_heartbeat()

      assert opts[:server_assignor] == "range"
      assert opts[:instance_id] == "inst-1"
      assert opts[:subscribed_topic_regex] == "a.*"
      refute Keyword.has_key?(opts, :subscribed_topic_names)
    end

    test "every retry of the join keeps the member id generated at start" do
      quiet(fn ->
        start_group()
        {from, opts} = next_heartbeat()
        GenServer.reply(from, {:error, :coordinator_not_available})

        {from, retry} = next_heartbeat()
        assert retry[:member_id] == opts[:member_id]
        assert retry[:member_epoch] == 0
        GenServer.reply(from, {:error, :unknown_member_id})

        {_from, retry2} = next_heartbeat()
        assert retry2[:member_id] == opts[:member_id]
        assert retry2[:member_epoch] == 0
        assert retry2[:subscribed_topic_names] == ["a"]
        assert retry2[:topic_partitions] == []
      end)
    end

    test "emits member_joined" do
      attach_telemetry([[:kafka_ex, :consumer, :member_joined]])
      start_group()
      {from, opts} = next_heartbeat()
      reply_ok(from, opts, 3)

      assert_receive {:telemetry, [:kafka_ex, :consumer, :member_joined], %{count: 1}, meta}
      assert meta == %{group_id: @group, member_id: opts[:member_id], member_epoch: 3}
    end

    test "invalid options fail the start with {:invalid_option, reason}" do
      Process.flag(:trap_exit, true)
      {:ok, client} = ScriptedClient.start_link(self())

      cases = [
        {["a"], [rack_id: 7], :invalid_rack_id},
        {["a"], [rack_id: ""], :invalid_rack_id},
        {["a"], [server_assignor: ""], :invalid_server_assignor},
        {["a"], [rebalance_timeout_ms: -1], :invalid_rebalance_timeout_ms},
        {["a"], [heartbeat_retry_backoff_ms: :x], :invalid_heartbeat_retry_backoff_ms},
        {["a"], [group_instance_id: " padded "], :invalid_group_instance_id},
        {["a"], [subscribed_topic_regex: 1], :invalid_subscribed_topic_regex},
        {[], [], :empty_subscription},
        {[:not_a_topic], [], :invalid_topics}
      ]

      for {topics, bad, reason} <- cases do
        opts = [client: client, test_pid: self(), __consumer_protocol_manager__: true] ++ bad

        quiet(fn ->
          assert {:error, {:shutdown, {:failed_to_start_child, ConsumerProtocolManager, {:invalid_option, ^reason}}}} =
                   ConsumerGroup.start_link({FakeConsumer, NoopConsumer}, @group, topics, opts)
        end)
      end
    end
  end

  # --- rack awareness (optional) ------------------------------------------------

  # The request the real client builds from these options (the RequestBuilder
  # call KafkaEx.Client makes), at ConsumerGroupHeartbeat v1.
  defp build_wire_request(opts) do
    state = %ClientState{api_versions: %{68 => {0, 1}}, correlation_id: 1}
    RequestBuilder.consumer_group_heartbeat_request([{:group_id, @group} | opts], state)
  end

  describe "rack awareness is optional" do
    test "without :rack_id no rack id is sent: the wire request carries null" do
      %{manager: manager} = start_group()
      {from, join} = next_heartbeat()
      refute Keyword.has_key?(join, :rack_id)
      assert {:ok, %{rack_id: nil}} = build_wire_request(join)

      reply_ok(from, join, 1)
      {from, hb} = poke(manager)
      refute Keyword.has_key?(hb, :rack_id)
      reply_ok(from, hb, 1)
    end

    test "a configured :rack_id is sent on the join and reaches the wire" do
      %{manager: manager} = start_group(rack_id: "use1-az1")
      {from, join} = next_heartbeat()
      assert join[:rack_id] == "use1-az1"

      assert {:ok, request} = build_wire_request(join)
      assert request.rack_id == "use1-az1"
      wire = %{request | correlation_id: 1, client_id: "test"} |> Kayrock.Request.serialize() |> IO.iodata_to_binary()
      assert :binary.match(wire, "use1-az1") != :nomatch

      # Later heartbeats leave it out (= unchanged), like Java.
      reply_ok(from, join, 1)
      {from, hb} = poke(manager)
      refute Keyword.has_key?(hb, :rack_id)
      reply_ok(from, hb, 1)

      # A full request after an error sends it again.
      quiet(fn ->
        {from, _hb} = poke(manager)
        GenServer.reply(from, {:error, :not_coordinator})
        {from, retry} = next_heartbeat()
        assert retry[:rack_id] == "use1-az1"
        reply_ok(from, retry, 1)
      end)
    end
  end

  # --- heartbeat --------------------------------------------------------------

  describe "heartbeat" do
    test "uses the broker's heartbeat_interval_ms and sends only what changed" do
      %{manager: manager} = start_group()
      {member_id, _pids} = join_with([0, 1])

      {from, hb} = poke(manager)
      reply_ok(from, hb, 1, nil, 50)

      {from, steady} = next_heartbeat(500)
      assert steady[:member_id] == member_id
      assert steady[:member_epoch] == 1
      assert owned_partitions(steady) == :unchanged
      refute Keyword.has_key?(steady, :subscribed_topic_names)
      refute Keyword.has_key?(steady, :rebalance_timeout_ms)
      reply_ok(from, steady, 1, nil, @long_interval)

      refute_receive {:heartbeat_call, _, _, _}, 300
    end

    test "after a transient error the next heartbeat is a full request" do
      log =
        capture_log(fn ->
          %{manager: manager} = start_group()
          {member_id, _pids} = join_with([0])
          {from, _} = poke(manager)
          GenServer.reply(from, {:error, :not_coordinator})

          {from, retry} = next_heartbeat()
          assert retry[:member_id] == member_id
          assert retry[:member_epoch] == 1
          assert retry[:subscribed_topic_names] == ["a"]
          assert retry[:rebalance_timeout_ms] == 300_000
          assert owned_partitions(retry) == [{@id_a, 0}]
          reply_ok(from, retry, 1)
        end)

      assert log =~ ":not_coordinator"
    end

    test "a connection lost after the send (client send-once) is resent in full with the owned partitions" do
      # The client no longer resends it unchanged (the broker may have bumped
      # the epoch): the manager's retry must be a full request, which the
      # broker accepts with the previous epoch.
      quiet(fn ->
        %{manager: manager} = start_group()
        {member_id, _pids} = join_with([0])
        {from, steady} = poke(manager)
        assert owned_partitions(steady) == :unchanged
        GenServer.reply(from, {:error, :closed})

        {from, retry} = next_heartbeat()
        assert retry[:member_id] == member_id
        assert retry[:member_epoch] == 1
        assert owned_partitions(retry) == [{@id_a, 0}]
        reply_ok(from, retry, 2)
        :ok = await_state(manager, &(&1.member_epoch == 2 and &1.heartbeat_failures == 0))
      end)
    end

    test "topic_authorization_failed keeps the member in the group (Java: an error event, not fatal)" do
      log =
        capture_log(fn ->
          %{manager: manager} = start_group()
          {member_id, %{0 => pid}} = join_with([0])
          {from, _} = poke(manager)
          GenServer.reply(from, {:error, :topic_authorization_failed})

          {from, retry} = next_heartbeat()
          assert retry[:member_id] == member_id
          assert retry[:member_epoch] == 1
          assert retry[:subscribed_topic_names] == ["a"]
          reply_ok(from, retry, 1)

          assert GenServer.call(manager, :member_epoch) == 1
          assert Process.alive?(pid)
          refute_received {:terminated, ^pid, _, _}
        end)

      assert log =~ ":topic_authorization_failed"
    end

    test "transient errors back off and never give up while the member keeps its partitions" do
      quiet(fn ->
        %{manager: manager} = start_group()
        {_member_id, %{0 => pid}} = join_with([0])

        {from, _} = poke(manager)
        GenServer.reply(from, {:error, :coordinator_load_in_progress})

        for _ <- 1..5 do
          {from, _} = next_heartbeat()
          GenServer.reply(from, {:error, :timeout})
        end

        {from, retry} = next_heartbeat()
        reply_ok(from, retry, 1)
        assert Process.alive?(manager)
        assert Process.alive?(pid)
        refute_received {:terminated, ^pid, _, _}
      end)
    end
  end

  # --- reconciliation ---------------------------------------------------------

  describe "reconciliation" do
    test "revokes first, reports, then assigns; kept partitions keep running and get the new epoch" do
      attach_telemetry([[:kafka_ex, :consumer, :reconcile]])
      %{manager: manager, sup: sup} = start_group()
      {_member_id, pids} = join_with([0, 1, 2])
      assert_receive {:telemetry, [:kafka_ex, :consumer, :reconcile], %{revoked: 0, assigned: 3}, _}

      # New target at epoch 2: keep 1 and 2, lose 0, gain 3.
      {from, hb} = poke(manager)
      reply_ok(from, hb, 2, assignment([{@id_a, "a", [1, 2, 3]}]))

      p0 = pids[0]
      assert_receive {:terminated, ^p0, {"a", 0}, :shutdown}

      # The reduced owned set is reported before the new partition starts.
      {from, revoke_ack} = next_heartbeat()
      assert owned_partitions(revoke_ack) == [{@id_a, 1}, {@id_a, 2}]
      refute_received {:started, _, {"a", 3}, _}
      reply_ok(from, revoke_ack, 2)

      assert_receive {:started, _p3, {"a", 3}, opts}
      assert opts[:member_epoch] == 2

      for p <- [1, 2] do
        pid = pids[p]
        assert_receive {:membership, ^pid, {"a", ^p}, [member_epoch: 2]}
      end

      {from, ack} = next_heartbeat()
      assert owned_partitions(ack) == [{@id_a, 1}, {@id_a, 2}, {@id_a, 3}]
      reply_ok(from, ack, 2)

      assert_receive {:telemetry, [:kafka_ex, :consumer, :reconcile], %{revoked: 1, assigned: 1}, meta}
      assert meta.member_epoch == 2

      for p <- [1, 2], do: assert(Process.alive?(pids[p]))
      refute_received {:terminated, _, {"a", 1}, _}
      refute_received {:terminated, _, {"a", 2}, _}
      assert ConsumerGroup.assignments(sup) == [{"a", 1}, {"a", 2}, {"a", 3}]
    end

    test "an epoch bump without an assignment change only pushes the epoch" do
      %{manager: manager} = start_group()
      {_member_id, %{0 => pid}} = join_with([0])

      {from, hb} = poke(manager)
      reply_ok(from, hb, 5)
      assert_receive {:membership, ^pid, {"a", 0}, [member_epoch: 5]}
      refute_receive {:heartbeat_call, _, _, _}, 200
      assert Process.alive?(pid)
    end

    test "a newer target that arrives during a revoke is reconciled after it" do
      %{manager: manager} = start_group()
      {_member_id, pids} = join_with([0, 1, 2])
      :ok = GenServer.call(pids[0], :block_stop)

      {from, hb} = poke(manager)
      reply_ok(from, hb, 2, assignment([{@id_a, "a", [1, 2]}]))
      p0 = pids[0]
      assert_receive {:stop_blocked, ^p0, {"a", 0}}

      # While 0 is still stopping, the broker moves on: now only [2] plus 3.
      {from, hb} = poke(manager)
      assert owned_partitions(hb) == :unchanged
      reply_ok(from, hb, 3, assignment([{@id_a, "a", [2, 3]}]))
      refute_receive {:terminated, _, {"a", 1}, _}, 100

      send(p0, :release_stop)
      assert_receive {:terminated, ^p0, {"a", 0}, :shutdown}
      p1 = pids[1]
      assert_receive {:terminated, ^p1, {"a", 1}, :shutdown}

      # Revoke reported before 3 is assigned.
      revoke_ack = drain_until_owned([{@id_a, 2}])
      assert revoke_ack[:member_epoch] == 3
      assert_receive {:started, _, {"a", 3}, opts}
      assert opts[:member_epoch] == 3

      ack = drain_until_owned([{@id_a, 2}, {@id_a, 3}])
      assert ack[:member_epoch] == 3
      refute_received {:started, _, {"a", 1}, _}
    end

    test "a partition revoked while it is being restarted is not started again" do
      quiet(fn ->
        %{manager: manager} = start_group(partition_restart_backoff_ms: 300, partition_restart_max_backoff_ms: 300)
        {_member_id, pids} = join_with([0, 1])
        :ok = GenServer.call(pids[0], {:exit, :boom})

        {from, hb} = poke(manager)
        reply_ok(from, hb, 2, assignment([{@id_a, "a", [1]}]))
        drain_until_owned([{@id_a, 1}])
        refute_receive {:started, _, {"a", 0}, _}, 500
      end)
    end

    test "an epoch push stuck behind a long fetch does not hold back a revoke" do
      %{manager: manager} = start_group()
      {_member_id, pids} = join_with([0, 1])
      :ok = GenServer.call(pids[1], :block_update)

      {from, hb} = poke(manager)
      reply_ok(from, hb, 2)
      p1 = pids[1]
      assert_receive {:update_blocked, ^p1, {"a", 1}}

      {from, hb} = poke(manager)
      reply_ok(from, hb, 3, assignment([{@id_a, "a", [1]}]))
      p0 = pids[0]
      assert_receive {:terminated, ^p0, {"a", 0}, :shutdown}
      {from, revoke_ack} = next_heartbeat()
      assert owned_partitions(revoke_ack) == [{@id_a, 1}]
      reply_ok(from, revoke_ack, 3)

      # The blocked update (epoch 2) completes; the consumer is then moved on
      # to epoch 3 (block again for the second update, released at once).
      send(p1, :release_update)
      assert_receive {:membership, ^p1, {"a", 1}, [member_epoch: 2]}
      assert_receive {:update_blocked, ^p1, {"a", 1}}, 1_000
      send(p1, :release_update)
      assert_receive {:membership, ^p1, {"a", 1}, [member_epoch: 3]}
    end

    test "an epoch push stuck behind a long fetch does not hold back an assign" do
      %{manager: manager} = start_group()
      {_member_id, %{0 => p0}} = join_with([0])
      :ok = GenServer.call(p0, :block_update)

      {from, hb} = poke(manager)
      reply_ok(from, hb, 2)
      assert_receive {:update_blocked, ^p0, {"a", 0}}

      {from, hb} = poke(manager)
      reply_ok(from, hb, 3, assignment([{@id_a, "a", [0, 1]}]))
      assert_receive {:started, _, {"a", 1}, opts}
      assert opts[:member_epoch] == 3
      send(p0, :release_update)
    end

    test "a crashed partition dropped from the target while a revoke runs is not restarted" do
      quiet(fn ->
        %{manager: manager} = start_group(partition_restart_backoff_ms: 100, partition_restart_max_backoff_ms: 100)
        {_member_id, pids} = join_with([0, 1, 2])
        :ok = GenServer.call(pids[1], :block_stop)

        {from, hb} = poke(manager)
        reply_ok(from, hb, 2, assignment([{@id_a, "a", [0, 2]}]))
        p1 = pids[1]
        assert_receive {:stop_blocked, ^p1, _}

        # 0 crashes (restart in 100 ms), then the broker drops it while the
        # revoke of 1 is still running.
        :ok = GenServer.call(pids[0], {:exit, :boom})
        {from, hb} = poke(manager)
        reply_ok(from, hb, 3, assignment([{@id_a, "a", [2]}]))

        refute_receive {:started, _, {"a", 0}, _}, 400
        send(p1, :release_stop)
        drain_until_owned([{@id_a, 2}])
        refute_received {:started, _, {"a", 0}, _}
      end)
    end

    test "a crashed partition dropped from the target and put back while a revoke runs is restarted" do
      quiet(fn ->
        %{manager: manager} = start_group(partition_restart_backoff_ms: 100, partition_restart_max_backoff_ms: 100)
        {member_id, pids} = join_with([0, 1, 2])
        :ok = GenServer.call(pids[1], :block_stop)

        {from, hb} = poke(manager)
        reply_ok(from, hb, 2, assignment([{@id_a, "a", [0, 2]}]))
        p1 = pids[1]
        assert_receive {:stop_blocked, ^p1, _}

        # 0 crashes, then leaves the target while the revoke of 1 runs; its
        # restart timer fires while it is out of the target.
        :ok = GenServer.call(pids[0], {:exit, :boom})
        {from, hb} = poke(manager)
        reply_ok(from, hb, 3, assignment([{@id_a, "a", [2]}]))
        refute_receive {:started, _, {"a", 0}, _}, 300

        # Before the revoke ends, the broker gives 0 back.
        {from, hb} = poke(manager)
        reply_ok(from, hb, 4, assignment([{@id_a, "a", [0, 2]}]))
        send(p1, :release_stop)

        assert_receive {:started, new_p0, {"a", 0}, opts}, 2_000
        assert opts[:member_epoch] == 4
        assert opts[:member_id] == member_id
        drain_until_owned([{@id_a, 0}, {@id_a, 2}])
        assert %{status: :running, pid: ^new_p0} = :sys.get_state(manager).owned[{@id_a, 0}]
      end)
    end

    test "a heartbeat due now is not pushed back by a reply that arrives first" do
      %{manager: manager} = start_group()
      join_with([0])
      {from, hb} = poke(manager)

      # Deterministic interleaving: the reply's :DOWN is queued before a
      # 0 ms heartbeat timer fires.
      :ok = :sys.suspend(manager)
      reply_ok(from, hb, 1)
      Process.sleep(50)

      :sys.replace_state(manager, fn state ->
        ref = make_ref()
        %{state | heartbeat_timer: {ref, Process.send_after(manager, {:heartbeat, ref}, 0)}}
      end)

      :ok = :sys.resume(manager)
      {from, hb} = next_heartbeat(500)
      reply_ok(from, hb, 1)
    end

    test "a revoked partition without a running consumer is reported with the rest before assigning" do
      quiet(fn ->
        %{manager: manager} = start_group()
        {_member_id, pids} = join_with([0, 1, 2])
        # 0 stops cleanly (not restarted), 1 is revoked while running.
        :ok = GenServer.call(pids[0], {:exit, :normal})
        :ok = GenServer.call(pids[1], :block_stop)

        {from, hb} = poke(manager)
        reply_ok(from, hb, 2, assignment([{@id_a, "a", [2, 3]}]))
        p1 = pids[1]
        assert_receive {:stop_blocked, ^p1, _}
        send(p1, :release_stop)

        # The first report after the revoke owns only 2: neither 0 nor 1.
        {from, report} = next_heartbeat()
        assert owned_partitions(report) == [{@id_a, 2}]
        refute_received {:started, _, {"a", 3}, _}
        reply_ok(from, report, 2)
        assert_receive {:started, _, {"a", 3}, _}
        drain_until_owned([{@id_a, 2}, {@id_a, 3}])
      end)
    end

    test "concurrent revokes stop several consumers at once" do
      %{manager: manager} = start_group()
      {_member_id, pids} = join_with([0, 1, 2])
      for p <- [0, 1], do: :ok = GenServer.call(pids[p], :block_stop)

      {from, hb} = poke(manager)
      reply_ok(from, hb, 2, assignment([{@id_a, "a", [2]}]))
      p0 = pids[0]
      p1 = pids[1]
      # Both are inside terminate/2 at the same time.
      assert_receive {:stop_blocked, ^p0, _}
      assert_receive {:stop_blocked, ^p1, _}
      send(p0, :release_stop)
      send(p1, :release_stop)
      drain_until_owned([{@id_a, 2}])
    end
  end

  # Waits (bounded) until the manager owns `pid` as a running consumer, so an
  # exit the test triggers next happens after the manager monitors it.
  defp await_owned(manager, pid, attempts \\ 50) do
    owned? = Enum.any?(:sys.get_state(manager).owned, fn {_key, entry} -> entry.pid == pid end)

    cond do
      owned? -> :ok
      attempts == 0 -> :timeout
      true -> Process.sleep(10) && await_owned(manager, pid, attempts - 1)
    end
  end

  # Moves a partition entry's last start `ms` into the past.
  defp age(manager, key, ms) do
    :sys.replace_state(manager, fn state ->
      %{state | owned: Map.update!(state.owned, key, &%{&1 | started_at: &1.started_at - ms})}
    end)
  end

  defp count_messages(msg, n \\ 0) do
    receive do
      ^msg -> count_messages(msg, n + 1)
    after
      0 -> n
    end
  end

  # Answers heartbeats until one reports `owned`; returns it.
  defp drain_until_owned(owned, attempts \\ 10) do
    {from, hb} = next_heartbeat()
    reply_ok(from, hb, hb[:member_epoch])

    cond do
      owned_partitions(hb) == owned -> hb
      attempts == 0 -> flunk("owned set never reported: #{inspect(owned)}")
      true -> drain_until_owned(owned, attempts - 1)
    end
  end

  # --- topic id resolution ------------------------------------------------------

  describe "topic ids without a name" do
    test "are resolved with a Metadata request by id; the partition is assigned once named" do
      %{manager: manager} = start_group()
      {_member_id, _pids} = join_with([0])

      {from, hb} = poke(manager)
      reply_ok(from, hb, 2, assignment([{@id_a, "a", [0]}, {@id_b, nil, [7]}]))

      assert_receive {:metadata_call, mfrom, mopts}
      assert mopts[:topic_ids] == [@id_b]

      metadata = %ClusterMetadata{topics: %{"b" => %Topic{name: "b", topic_id: @id_b}}}
      GenServer.reply(mfrom, {:ok, metadata})

      assert_receive {:started, _, {"b", 7}, opts}
      assert opts[:member_epoch] == 2
      drain_until_owned([{@id_a, 0}, {@id_b, 7}])
    end

    test "an unknown topic id is never assigned; the lookup is retried and the rest is acked" do
      quiet(fn ->
        %{manager: manager} = start_group()
        {_member_id, _pids} = join_with([0])

        {from, hb} = poke(manager)
        reply_ok(from, hb, 2, assignment([{@id_a, "a", [0]}, {@id_b, nil, [7]}]))

        assert_receive {:metadata_call, mfrom, _}

        GenServer.reply(
          mfrom,
          {:error, %KafkaEx.Client.Error{error: :unknown_topic_id, metadata: %{topic_ids: [@id_b]}}}
        )

        # The owned set did not change (no ack needed); the unnamed
        # partition is not assigned.
        refute_receive {:started, _, {_, 7}, _}, 5

        # Retried after the backoff; now the topic is known.
        assert_receive {:metadata_call, mfrom, mopts}, 1_000
        assert mopts[:topic_ids] == [@id_b]
        GenServer.reply(mfrom, {:ok, %ClusterMetadata{topics: %{"b" => %Topic{name: "b", topic_id: @id_b}}}})

        assert_receive {:started, _, {"b", 7}, _}
        drain_until_owned([{@id_a, 0}, {@id_b, 7}])
      end)
    end

    # The by-id Metadata error carries only the first failure's code, with
    # every failed id: an unauthorized topic listed first must not keep the
    # other ids unnamed.
    test "ids of a failed lookup are skipped whatever the error code; the others are looked up again" do
      quiet(fn ->
        id_c = <<3::128>>
        %{manager: manager} = start_group()
        {_member_id, _pids} = join_with([0])

        {from, hb} = poke(manager)
        reply_ok(from, hb, 2, assignment([{@id_a, "a", [0]}, {@id_b, nil, [7]}, {id_c, nil, [9]}]))

        assert_receive {:metadata_call, mfrom, mopts}
        assert Enum.sort(mopts[:topic_ids]) == [@id_b, id_c]

        GenServer.reply(
          mfrom,
          {:error,
           %KafkaEx.Client.Error{
             error: :topic_authorization_failed,
             metadata: %{topic_ids: [@id_b], topic_errors: [{@id_b, :topic_authorization_failed}]}
           }}
        )

        # At once, not after the backoff, and only for the other id.
        assert_receive {:metadata_call, mfrom, mopts}, 5
        assert mopts[:topic_ids] == [id_c]
        GenServer.reply(mfrom, {:ok, %ClusterMetadata{topics: %{"c" => %Topic{name: "c", topic_id: id_c}}}})

        assert_receive {:started, _, {"c", 9}, opts}
        assert opts[:member_epoch] == 2
        refute_received {:started, _, {_, 7}, _}
        drain_until_owned([{@id_a, 0}, {id_c, 9}])
      end)
    end

    test "a lookup that hangs never blocks the manager: it keeps heartbeating and handling consumers" do
      quiet(fn ->
        %{manager: manager, sup: sup} = start_group()
        {_member_id, %{0 => p0}} = join_with([0])

        {from, hb} = poke(manager)
        reply_ok(from, hb, 2, assignment([{@id_a, "a", [0]}, {@id_b, nil, [7]}]))

        # The Metadata call is left unanswered (a slow or unreachable broker).
        assert_receive {:metadata_call, mfrom, mopts}
        assert mopts[:topic_ids] == [@id_b]

        # The manager still answers calls...
        assert GenServer.call(manager, :member_epoch, 500) == 2
        assert ConsumerGroup.assignments(sup) == [{"a", 0}]

        # ...heartbeats...
        {from, hb} = poke(manager)
        assert hb[:member_epoch] == 2
        reply_ok(from, hb, 2)

        # ...and restarts a crashed consumer.
        :ok = GenServer.call(p0, {:exit, :boom})
        assert_receive {:started, _, {"a", 0}, opts}, 1_000
        assert opts[:member_epoch] == 2

        # No second lookup while the first is in flight.
        refute_receive {:metadata_call, _, _}, 100

        # Once it answers, the partition is assigned.
        GenServer.reply(mfrom, {:ok, %ClusterMetadata{topics: %{"b" => %Topic{name: "b", topic_id: @id_b}}}})
        assert_receive {:started, _, {"b", 7}, _}
        drain_until_owned([{@id_a, 0}, {@id_b, 7}])
      end)
    end

    test "a lookup that hangs does not delay the shutdown" do
      quiet(fn ->
        %{manager: manager, sup: sup} = start_group()
        {_member_id, %{0 => p0}} = join_with([0])
        {from, hb} = poke(manager)
        reply_ok(from, hb, 2, assignment([{@id_a, "a", [0]}, {@id_b, nil, [7]}]))
        assert_receive {:metadata_call, _mfrom, _}

        started = System.monotonic_time(:millisecond)
        stop_async(sup)
        assert_receive {:terminated, ^p0, {"a", 0}, :shutdown}, 1_000
        {from, leave} = next_heartbeat()
        assert leave[:member_epoch] == -1
        reply_ok(from, leave, -1)
        assert_receive {:sup_down, :shutdown}, 5_000
        assert System.monotonic_time(:millisecond) - started < 2_000
      end)
    end

    test "a failed lookup is retried only after :topic_resolution_backoff_ms" do
      quiet(fn ->
        %{manager: manager} = start_group(topic_resolution_backoff_ms: 400, topic_resolution_max_backoff_ms: 400)
        join_with([0])
        {from, hb} = poke(manager)
        reply_ok(from, hb, 1, assignment([{@id_a, "a", [0]}, {@id_b, nil, [7]}]))
        assert_receive {:metadata_call, mfrom, _}
        GenServer.reply(mfrom, {:error, :timeout})

        # Not at once (a hot loop against the brokers), but after the backoff.
        refute_receive {:metadata_call, _, _}, 250
        assert_receive {:metadata_call, mfrom, mopts}, 1_000
        assert mopts[:topic_ids] == [@id_b]
        GenServer.reply(mfrom, {:ok, %ClusterMetadata{topics: %{"b" => %Topic{name: "b", topic_id: @id_b}}}})
        assert_receive {:started, _, {"b", 7}, _}
      end)
    end

    test "a failed lookup (exit or error) never crashes the member" do
      quiet(fn ->
        %{manager: manager} = start_group()
        join_with([0])
        {from, hb} = poke(manager)
        reply_ok(from, hb, 2, assignment([{@id_a, "a", [0]}, {@id_b, nil, [1]}]))
        assert_receive {:metadata_call, mfrom, _}
        GenServer.reply(mfrom, {:error, :api_version_no_supported})
        # Retried after the backoff.
        assert_receive {:metadata_call, mfrom, _}, 1_000
        GenServer.reply(mfrom, {:error, :timeout})
        assert_receive {:metadata_call, _mfrom, _}, 1_000
        assert Process.alive?(manager)
        assert Process.alive?(manager)
      end)
    end
  end

  # --- fencing ----------------------------------------------------------------

  describe "fencing" do
    test "fenced_member_epoch: partitions lost without commit, rejoin with epoch 0 and the same member id" do
      attach_telemetry([[:kafka_ex, :consumer, :member_fenced]])

      quiet(fn ->
        %{manager: manager} = start_group()
        {member_id, pids} = join_with([0, 1])

        {from, _} = poke(manager)
        GenServer.reply(from, {:error, :fenced_member_epoch})

        for p <- [0, 1] do
          pid = pids[p]
          assert_receive {:terminated, ^pid, {"a", ^p}, {:shutdown, :partition_lost}}
        end

        assert_receive {:telemetry, [:kafka_ex, :consumer, :member_fenced], %{count: 1, lost: 2}, meta}
        assert meta.reason == :fenced_member_epoch

        {from, rejoin} = next_heartbeat()
        assert rejoin[:member_epoch] == 0
        assert rejoin[:member_id] == member_id
        assert rejoin[:topic_partitions] == []
        assert rejoin[:subscribed_topic_names] == ["a"]

        reply_ok(from, rejoin, 4, assignment([{@id_a, "a", [0]}]))
        assert_receive {:started, _, {"a", 0}, opts}
        assert opts[:member_epoch] == 4
      end)
    end

    test "unknown_member_id fences too (Java transitionToFenced), keeping the member id" do
      quiet(fn ->
        %{manager: manager} = start_group()
        {member_id, %{0 => pid}} = join_with([0])
        {from, _} = poke(manager)
        GenServer.reply(from, {:error, :unknown_member_id})
        assert_receive {:terminated, ^pid, {"a", 0}, {:shutdown, :partition_lost}}
        {_from, rejoin} = next_heartbeat()
        assert rejoin[:member_epoch] == 0
        assert rejoin[:member_id] == member_id
      end)
    end

    test "fenced in the middle of a revoke: the rest is lost, the revoke finishes, then rejoin" do
      quiet(fn ->
        %{manager: manager} = start_group()
        {member_id, pids} = join_with([0, 1])
        :ok = GenServer.call(pids[0], :block_stop)

        {from, hb} = poke(manager)
        reply_ok(from, hb, 2, assignment([{@id_a, "a", [1]}]))
        p0 = pids[0]
        assert_receive {:stop_blocked, ^p0, _}

        # A heartbeat while the revoke is under way is fenced.
        {from, hb} = poke(manager)
        assert owned_partitions(hb) == :unchanged
        GenServer.reply(from, {:error, :fenced_member_epoch})

        # No rejoin while the revoke is still stopping consumer 0.
        refute_receive {:heartbeat_call, _, _, _}, 200
        send(p0, :release_stop)
        assert_receive {:terminated, ^p0, {"a", 0}, :shutdown}

        p1 = pids[1]
        assert_receive {:terminated, ^p1, {"a", 1}, {:shutdown, :partition_lost}}

        {_from, rejoin} = next_heartbeat()
        assert rejoin[:member_epoch] == 0
        assert rejoin[:member_id] == member_id
        assert rejoin[:topic_partitions] == []
      end)
    end

    test "fenced while a crashed consumer restarts: the rejoin waits for the start, then loses it" do
      quiet(fn ->
        %{manager: manager} = start_group()
        {member_id, %{0 => p0}} = join_with([0])
        FakeConsumer.block_init(self(), {"a", 0})
        :ok = GenServer.call(p0, {:exit, :boom})
        assert_receive {:init_blocked, new_p0, {"a", 0}}, 1_000

        {from, _} = poke(manager)
        GenServer.reply(from, {:error, :fenced_member_epoch})

        # Rejoining now would orphan the consumer being started (at the
        # fenced epoch, owned by nobody).
        refute_receive {:heartbeat_call, _, _, _}, 300
        send(new_p0, :release_init)
        assert_receive {:started, ^new_p0, {"a", 0}, _}
        assert_receive {:terminated, ^new_p0, {"a", 0}, {:shutdown, :partition_lost}}, 1_000

        {_from, rejoin} = next_heartbeat()
        assert rejoin[:member_epoch] == 0
        assert rejoin[:member_id] == member_id
        assert rejoin[:topic_partitions] == []
        refute Process.alive?(new_p0)
      end)
    end
  end

  describe "shutdown during a revoke" do
    test "a consumer the revoke is stopping is stopped once, with the revoke's reason; the others too" do
      %{manager: manager} = start_group()
      {_member_id, pids} = join_with([0, 1])
      p0 = pids[0]
      p1 = pids[1]
      :ok = GenServer.call(p0, :block_stop)

      {from, hb} = poke(manager)
      reply_ok(from, hb, 2, assignment([{@id_a, "a", [1]}]))
      assert_receive {:stop_blocked, ^p0, _}

      # The manager's own terminate/2 (stopping the whole group would stop
      # the consumer supervisor, a later sibling, first).
      ref = Process.monitor(manager)
      spawn(fn -> GenServer.stop(manager, :shutdown, 10_000) end)

      # The kept partition is stopped (and commits) while the revoke waits.
      assert_receive {:terminated, ^p1, {"a", 1}, :shutdown}, 1_000
      refute_received {:terminated, ^p0, _, _}

      send(p0, :release_stop)
      assert_receive {:terminated, ^p0, {"a", 0}, :shutdown}
      {from, leave} = next_heartbeat()
      assert leave[:member_epoch] == -1
      reply_ok(from, leave, -1)
      assert_receive {:DOWN, ^ref, :process, ^manager, :shutdown}, 5_000
      refute_received {:terminated, ^p0, _, _}
      refute_received {:started, _, _, _}
    end
  end

  describe "shutdown during an assign" do
    test "consumers the assign in flight starts after the stop began are stopped too" do
      quiet(fn ->
        %{manager: manager} = start_group()
        {_member_id, %{0 => p0}} = join_with([0])
        FakeConsumer.block_init(self(), {"a", 1})

        {from, hb} = poke(manager)
        reply_ok(from, hb, 2, assignment([{@id_a, "a", [0, 1]}]))
        assert_receive {:init_blocked, p1, {"a", 1}}

        ref = Process.monitor(manager)
        spawn(fn -> GenServer.stop(manager, :shutdown, 10_000) end)
        Process.sleep(100)
        send(p1, :release_init)
        assert_receive {:started, ^p1, {"a", 1}, _}

        # Both are stopped by the manager itself, before its leave (not by
        # the group supervisor's teardown once the manager is gone).
        {from, leave} = next_heartbeat()
        assert leave[:member_epoch] == -1
        assert_received {:terminated, ^p0, {"a", 0}, :shutdown}
        assert_received {:terminated, ^p1, {"a", 1}, :shutdown}
        reply_ok(from, leave, -1)
        assert_receive {:DOWN, ^ref, :process, ^manager, :shutdown}, 5_000
      end)
    end
  end

  describe "shutdown during a restart" do
    test "a consumer whose restart is in flight is stopped with its commit before the leave" do
      quiet(fn ->
        %{manager: manager} = start_group()
        {member_id, %{0 => p0}} = join_with([0])
        FakeConsumer.block_init(self(), {"a", 0})
        :ok = GenServer.call(p0, {:exit, :boom})
        assert_receive {:init_blocked, new_p0, {"a", 0}}, 1_000

        ref = Process.monitor(manager)
        spawn(fn -> GenServer.stop(manager, :shutdown, 10_000) end)
        # Its shutdown waits for the start in flight.
        :ok = await_blocked_in_terminate(manager)
        send(new_p0, :release_init)
        assert_receive {:started, ^new_p0, {"a", 0}, _}

        # Stopped (:shutdown = the final commit) before the leave is sent.
        assert {:terminated, {"a", 0}, :shutdown} = next_terminated_or_heartbeat(new_p0)
        {from, leave} = next_heartbeat()
        assert leave[:member_epoch] == -1
        assert leave[:member_id] == member_id
        reply_ok(from, leave, -1)
        assert_receive {:DOWN, ^ref, :process, ^manager, :shutdown}, 5_000
      end)
    end
  end

  describe "shutdown keepalive" do
    test "a heartbeat in flight at shutdown is awaited; the keepalive sends its new epoch and the owned set" do
      quiet(fn ->
        %{manager: manager} = start_group()
        {member_id, %{0 => p0}} = join_with([0])
        :ok = await_state(manager, &(&1.inflight == nil))
        :ok = GenServer.call(p0, :block_stop)

        # A heartbeat is in flight when the shutdown starts.
        {from, hb} = poke(manager)
        assert hb[:member_epoch] == 1
        ref = Process.monitor(manager)
        spawn(fn -> GenServer.stop(manager, :shutdown, 30_000) end)
        :ok = await_blocked_in_terminate(manager)

        # Its reply moves the member to epoch 2, with a 100 ms interval.
        reply_ok(from, hb, 2, nil, 100)
        assert_receive {:stop_blocked, ^p0, {"a", 0}}, 2_000

        {from, keepalive} = next_heartbeat(1_000)
        # pre-fix: epoch 1 (the aborted reply was lost), no owned partitions: fenced
        assert keepalive[:member_epoch] == 2
        assert keepalive[:member_id] == member_id
        assert owned_partitions(keepalive) == [{@id_a, 0}]
        reply_ok(from, keepalive, 2, nil, 100)

        send(p0, :release_stop)
        assert_receive {:terminated, ^p0, {"a", 0}, :shutdown}
        {from, leave} = drain_until_leave()
        assert leave[:member_epoch] == -1
        reply_ok(from, leave, -1)
        assert_receive {:DOWN, ^ref, :process, ^manager, _}, 5_000
      end)
    end

    test "a fencing reply to the heartbeat in flight at shutdown: consumers lost, no keepalive, no leave" do
      quiet(fn ->
        %{manager: manager} = start_group()
        {_member_id, %{0 => p0}} = join_with([0])
        :ok = await_state(manager, &(&1.inflight == nil))
        :sys.replace_state(manager, &%{&1 | heartbeat_interval_ms: 100})

        {from, _hb} = poke(manager)
        ref = Process.monitor(manager)
        spawn(fn -> GenServer.stop(manager, :shutdown, 30_000) end)
        :ok = await_blocked_in_terminate(manager)
        GenServer.reply(from, {:error, :fenced_member_epoch})

        # Lost: stopped without a commit (GenConsumerSupervisor lost: true).
        assert_receive {:terminated, ^p0, {"a", 0}, reason}, 2_000
        assert reason != :shutdown
        assert_receive {:DOWN, ^ref, :process, ^manager, _}, 5_000
        refute_received {:heartbeat_call, _, _, _}
      end)
    end

    test "a new member epoch in a keepalive reply is sent on the next keepalive" do
      quiet(fn ->
        %{manager: manager} = start_group()
        {member_id, pids} = join_with([0])
        p0 = pids[0]

        # A 100 ms broker interval for the keepalive, while the manager's own
        # heartbeat timer (set from a long interval) never fires. The ack's
        # reply is handled first, or it would set the interval back.
        :ok = await_state(manager, &(&1.inflight == nil and &1.heartbeat_interval_ms == @long_interval))
        :sys.replace_state(manager, &%{&1 | heartbeat_interval_ms: 100})
        :ok = GenServer.call(p0, :block_stop)
        ref = Process.monitor(manager)
        spawn(fn -> GenServer.stop(manager, :shutdown, 30_000) end)
        assert_receive {:stop_blocked, ^p0, {"a", 0}}, 2_000

        {from, hb} = next_heartbeat(1_000)
        assert hb[:member_epoch] == 1
        assert hb[:member_id] == member_id
        # The broker moves the member on to epoch 2 meanwhile.
        reply_ok(from, hb, 2, nil, 100)

        {from, hb} = next_heartbeat(1_000)
        assert hb[:member_epoch] == 2
        assert hb[:member_id] == member_id
        reply_ok(from, hb, 2, nil, 100)

        send(p0, :release_stop)
        assert_receive {:terminated, ^p0, {"a", 0}, :shutdown}
        {from, leave} = drain_until_leave()
        assert leave[:member_epoch] == -1
        reply_ok(from, leave, -1)
        assert_receive {:DOWN, ^ref, :process, ^manager, _}, 5_000
      end)
    end

    test "a terminal reply to the heartbeat in flight at shutdown: consumers lost, no keepalive, no leave" do
      quiet(fn ->
        %{manager: manager} = start_group()
        {_member_id, %{0 => p0}} = join_with([0])
        :ok = await_state(manager, &(&1.inflight == nil))
        :sys.replace_state(manager, &%{&1 | heartbeat_interval_ms: 100})

        {from, _hb} = poke(manager)
        ref = Process.monitor(manager)
        spawn(fn -> GenServer.stop(manager, :shutdown, 30_000) end)
        :ok = await_blocked_in_terminate(manager)
        GenServer.reply(from, {:error, :group_authorization_failed})

        assert_receive {:terminated, ^p0, {"a", 0}, {:shutdown, :partition_lost}}, 2_000
        assert_receive {:DOWN, ^ref, :process, ^manager, _}, 5_000
        refute_received {:heartbeat_call, _, _, _}
      end)
    end
  end

  # --- consumer exits -----------------------------------------------------------

  describe "consumer exits" do
    test "a crashed consumer is restarted with the current epoch" do
      quiet(fn ->
        %{manager: manager} = start_group()
        {_member_id, %{0 => pid}} = join_with([0])
        {from, hb} = poke(manager)
        reply_ok(from, hb, 2)
        assert_receive {:membership, ^pid, _, [member_epoch: 2]}

        :ok = GenServer.call(pid, {:exit, :boom})
        assert_receive {:started, new_pid, {"a", 0}, opts}
        assert new_pid != pid
        assert opts[:member_epoch] == 2
      end)
    end

    test "a stale-epoch commit does not kill the group (the consumer stays up)" do
      # GenConsumer keeps running on :stale_member_epoch (chunk C), so the
      # manager sees nothing; a {:rejoin_required, ...} cast only triggers a
      # heartbeat, it never fences on its own.
      %{manager: manager} = start_group()
      {_member_id, %{0 => pid}} = join_with([0])
      GenServer.cast(manager, {:rejoin_required, :fenced_member_epoch, 1})
      {from, hb} = next_heartbeat()
      assert hb[:member_epoch] == 1
      reply_ok(from, hb, 1)
      refute_receive {:terminated, ^pid, _, _}, 100
      assert Process.alive?(manager)
    end

    for reason <- [:normal, {:shutdown, :done}] do
      test "a consumer that stops on its own (#{inspect(reason)}) is reported and not restarted" do
        quiet(fn ->
          attach_telemetry([[:kafka_ex, :consumer, :partition_stopped]])
          %{manager: manager} = start_group()
          {member_id, %{0 => pid}} = join_with([0])

          :ok = GenServer.call(pid, {:exit, unquote(Macro.escape(reason))})

          assert_receive {:telemetry, [:kafka_ex, :consumer, :partition_stopped], %{count: 1}, meta}
          assert %{member_id: ^member_id, member_epoch: 1, topic: "a", partition: 0} = meta
          assert meta.reason == unquote(Macro.escape(reason))
          refute_receive {:started, _, {"a", 0}, _}, 300
          assert Process.alive?(manager)
        end)
      end
    end

    test "a consumer stopped with exactly :shutdown (a supervisor stop) is not reported" do
      quiet(fn ->
        attach_telemetry([[:kafka_ex, :consumer, :partition_stopped]])
        %{manager: manager} = start_group()
        {_member_id, %{0 => pid}} = join_with([0])

        :ok = GenServer.call(pid, {:exit, :shutdown})

        refute_receive {:telemetry, [:kafka_ex, :consumer, :partition_stopped], _, _}, 300
        refute_receive {:started, _, {"a", 0}, _}
        assert Process.alive?(manager)
      end)
    end

    test "a consumer that exits with rejoin_required is restarted after a heartbeat" do
      quiet(fn ->
        %{manager: manager} = start_group()
        {_member_id, %{0 => pid}} = join_with([0])
        :ok = GenServer.call(pid, {:exit, {:shutdown, {:rejoin_required, :unknown_member_id}}})

        {from, hb} = next_heartbeat()
        reply_ok(from, hb, 1)
        assert_receive {:started, new_pid, {"a", 0}, opts}
        assert new_pid != pid
        assert opts[:member_epoch] == 1
        assert Process.alive?(manager)
      end)
    end

    test "a consumer's terminal error stops the member loudly; the others commit and it leaves" do
      Process.flag(:trap_exit, true)

      quiet(fn ->
        %{sup: sup} = start_group()
        ref = Process.monitor(sup)
        {_member_id, pids} = join_with([0, 1])
        :ok = GenServer.call(pids[0], {:exit, {:shutdown, {:terminal_error, :group_authorization_failed}}})

        p1 = pids[1]
        assert_receive {:terminated, ^p1, {"a", 1}, :shutdown}
        {from, leave} = next_heartbeat()
        assert leave[:member_epoch] == -1
        reply_ok(from, leave, -1)
        assert_receive {:DOWN, ^ref, :process, ^sup, _}, 5_000
      end)
    end

    # Java keeps heartbeating while it leaves (PREPARE_LEAVING is not a
    # state shouldSkipHeartbeat skips): a slow consumer stop must not
    # outlast the session timeout, or the broker hands the partition to
    # another member while this consumer still consumes it.
    for {name, how} <- [terminal_error: :terminal_error, stop: :stop] do
      test "heartbeats go on while the consumers stop on shutdown (#{name}), then it leaves" do
        Process.flag(:trap_exit, true)

        quiet(fn ->
          %{manager: manager} = start_group()
          {from, join} = next_heartbeat()
          reply_ok(from, join, 1, assignment([{@id_a, "a", [0, 1]}]), 100)
          assert_receive {:started, p0, {"a", 0}, _}
          assert_receive {:started, p1, {"a", 1}, _}
          drain_until_owned([{@id_a, 0}, {@id_a, 1}])
          {from, hb} = poke(manager)
          reply_ok(from, hb, 1, nil, 100)

          :ok = GenServer.call(p1, :block_stop)
          ref = Process.monitor(manager)

          case unquote(how) do
            :terminal_error -> :ok = GenServer.call(p0, {:exit, {:shutdown, {:terminal_error, :boom}}})
            :stop -> spawn(fn -> GenServer.stop(manager, :shutdown, 30_000) end)
          end

          assert_receive {:stop_blocked, ^p1, {"a", 1}}, 2_000

          # p1 still owns its partition with a valid epoch: the member keeps
          # heartbeating at the broker's interval.
          beats =
            for _ <- 1..3 do
              {from, hb} = next_heartbeat(1_000)
              reply_ok(from, hb, 1, nil, 100)
              hb
            end

          for hb <- beats do
            assert hb[:member_epoch] == 1
            assert hb[:member_id] == join[:member_id]
          end

          send(p1, :release_stop)
          assert_receive {:terminated, ^p1, {"a", 1}, :shutdown}

          {from, leave} = next_heartbeat()
          assert leave[:member_epoch] == -1
          reply_ok(from, leave, -1)
          assert_receive {:DOWN, ^ref, :process, ^manager, _}, 5_000
          refute_receive {:heartbeat_call, _, _, _}, 300
        end)
      end
    end

    test "a fenced member sends no heartbeat while its consumers stop on shutdown" do
      quiet(fn ->
        %{manager: manager} = start_group()
        {from, join} = next_heartbeat()
        reply_ok(from, join, 1, assignment([{@id_a, "a", [0]}]), 100)
        assert_receive {:started, p0, {"a", 0}, _}
        drain_until_owned([{@id_a, 0}])

        # A 100 ms interval: a keepalive would show up in the window below.
        {from, hb} = poke(manager)
        reply_ok(from, hb, 1, nil, 100)
        :ok = GenServer.call(p0, :block_stop)
        {from, _hb} = next_heartbeat(1_000)
        GenServer.reply(from, {:error, :fenced_member_epoch})
        assert_receive {:stop_blocked, ^p0, {"a", 0}}, 2_000

        ref = Process.monitor(manager)
        spawn(fn -> GenServer.stop(manager, :shutdown, 30_000) end)
        refute_receive {:heartbeat_call, _, _, _}, 400
        send(p0, :release_stop)
        assert_receive {:DOWN, ^ref, :process, ^manager, _}, 5_000
        refute_receive {:heartbeat_call, _, _, _}, 100
      end)
    end

    test "restarting a crashed consumer never blocks the manager while the consumer starts" do
      quiet(fn ->
        %{manager: manager, sup: sup} = start_group()
        {_member_id, %{0 => p0}} = join_with([0, 1])
        FakeConsumer.block_init(self(), {"a", 0})
        :ok = GenServer.call(p0, {:exit, :boom})
        assert_receive {:init_blocked, new_p0, {"a", 0}}, 1_000

        # The manager answers calls and heartbeats while the start hangs.
        assert GenServer.call(manager, :member_epoch, 500) == 1
        assert ConsumerGroup.assignments(sup) == [{"a", 0}, {"a", 1}]
        {from, hb} = poke(manager)
        assert hb[:member_epoch] == 1
        reply_ok(from, hb, 1)

        send(new_p0, :release_init)
        assert_receive {:started, ^new_p0, {"a", 0}, opts}
        assert opts[:member_epoch] == 1

        # The restarted consumer is owned and monitored: a later crash is
        # restarted again.
        :ok = GenServer.call(new_p0, {:exit, :boom})
        assert_receive {:started, third, {"a", 0}, _}, 1_000
        assert third != new_p0
      end)
    end

    test "a partition revoked while its consumer restarts is stopped once the start ends, then reported" do
      quiet(fn ->
        %{manager: manager} = start_group()
        {_member_id, pids} = join_with([0, 1])
        FakeConsumer.block_init(self(), {"a", 0})
        :ok = GenServer.call(pids[0], {:exit, :boom})
        assert_receive {:init_blocked, new_p0, {"a", 0}}, 1_000

        {from, hb} = poke(manager)
        reply_ok(from, hb, 2, assignment([{@id_a, "a", [1]}]))
        # Not reported revoked while its consumer may still come up.
        refute_receive {:heartbeat_call, _, _, _}, 200

        send(new_p0, :release_init)
        assert_receive {:terminated, ^new_p0, {"a", 0}, :shutdown}, 1_000
        drain_until_owned([{@id_a, 1}])
      end)
    end

    test "a consumer that exits with a terminal error while the assign still starts others stops the member" do
      quiet(fn ->
        %{sup: sup, manager: manager} = start_group()
        ref = Process.monitor(manager)
        FakeConsumer.block_init(self(), {"a", 1})
        {from, join} = next_heartbeat()
        reply_ok(from, join, 1, assignment([{@id_a, "a", [0, 1]}]))
        assert_receive {:started, p0, {"a", 0}, _}
        assert_receive {:init_blocked, p1, {"a", 1}}
        await_owned(manager, p0)

        :ok = GenServer.call(p0, {:exit, {:shutdown, {:terminal_error, :group_authorization_failed}}})
        send(p1, :release_init)

        assert_receive {:DOWN, ^ref, :process, ^manager, {:shutdown, {:terminal, :group_authorization_failed}}},
                       5_000

        refute_received {:started, _, {"a", 0}, _}
        _ = sup
      end)
    end

    test "a consumer that stops normally while the assign still starts others is not restarted" do
      quiet(fn ->
        %{manager: manager} = start_group()
        FakeConsumer.block_init(self(), {"a", 1})
        {from, join} = next_heartbeat()
        reply_ok(from, join, 1, assignment([{@id_a, "a", [0, 1]}]))
        assert_receive {:started, p0, {"a", 0}, _}
        assert_receive {:init_blocked, p1, {"a", 1}}
        await_owned(manager, p0)

        :ok = GenServer.call(p0, {:exit, :normal})
        send(p1, :release_init)
        assert_receive {:started, ^p1, {"a", 1}, _}
        refute_receive {:started, _, {"a", 0}, _}, 300
        assert Process.alive?(manager)
      end)
    end

    test "a failed epoch push is retried only after :partition_restart_backoff_ms" do
      quiet(fn ->
        %{manager: manager} = start_group(partition_restart_backoff_ms: 400, partition_restart_max_backoff_ms: 400)
        {_member_id, %{0 => p0}} = join_with([0])
        :ok = GenServer.call(p0, {:fail_update, 1})

        {from, hb} = poke(manager)
        reply_ok(from, hb, 2)
        assert_receive {:update_failed, ^p0, {"a", 0}}
        failed_at = System.monotonic_time(:millisecond)

        assert_receive {:membership, ^p0, {"a", 0}, [member_epoch: 2]}, 2_000
        assert System.monotonic_time(:millisecond) - failed_at >= 300
      end)
    end

    test "the restart backoff doubles across consecutive crashes" do
      quiet(fn ->
        %{manager: manager} = start_group(partition_restart_backoff_ms: 100, partition_restart_max_backoff_ms: 10_000)
        {_member_id, %{0 => pid}} = join_with([0])

        # Crashes 1 and 2 restart after 100 and 200 ms; crash 3 after 400.
        pid =
          Enum.reduce(1..2, pid, fn _, pid ->
            :ok = await_owned(manager, pid)
            :ok = GenServer.call(pid, {:exit, :boom})
            assert_receive {:started, new_pid, {"a", 0}, _}, 1_000
            new_pid
          end)

        :ok = await_owned(manager, pid)
        crashed_at = System.monotonic_time(:millisecond)
        :ok = GenServer.call(pid, {:exit, :boom})
        assert_receive {:started, _, {"a", 0}, _}, 2_000
        assert System.monotonic_time(:millisecond) - crashed_at >= 350
      end)
    end

    test "the restart backoff is reset once a consumer has run for a minute" do
      quiet(fn ->
        %{manager: manager} = start_group(partition_restart_backoff_ms: 200, partition_restart_max_backoff_ms: 10_000)
        {_member_id, %{0 => pid}} = join_with([0])

        pid =
          Enum.reduce(1..2, pid, fn _, pid ->
            :ok = await_owned(manager, pid)
            :ok = GenServer.call(pid, {:exit, :boom})
            assert_receive {:started, new_pid, {"a", 0}, _}, 2_000
            new_pid
          end)

        # Two crashes so far; the third would wait 800 ms, but the consumer
        # has now been up for over a minute: back to the base delay.
        :ok = await_owned(manager, pid)
        age(manager, {@id_a, 0}, 61_000)
        crashed_at = System.monotonic_time(:millisecond)
        :ok = GenServer.call(pid, {:exit, :boom})
        assert_receive {:started, _, {"a", 0}, _}, 2_000
        assert System.monotonic_time(:millisecond) - crashed_at < 600
      end)
    end

    test "a consumer whose start keeps failing backs off exponentially, also a minute after its last start" do
      quiet(fn ->
        %{manager: manager} = start_group(partition_restart_backoff_ms: 50, partition_restart_max_backoff_ms: 5_000)
        {_member_id, %{0 => pid}} = join_with([0])
        :ok = await_owned(manager, pid)

        # Its last good start is over a minute old; from now on every start fails.
        age(manager, {@id_a, 0}, 61_000)
        FakeConsumer.fail_init(self(), {"a", 0})
        :ok = GenServer.call(pid, {:exit, :boom})

        # Doubling from 50 ms: attempts at about 50, 150, 350, 750 and 1550 ms.
        Process.sleep(1_500)
        attempts = count_messages({:init_failed, {"a", 0}})
        assert attempts in 2..6, "#{attempts} failed starts in 1.5 s"
      end)
    end

    test "an assign worker that dies does not re-monitor the consumers it already started" do
      quiet(fn ->
        %{manager: manager} = start_group()
        FakeConsumer.block_init(self(), {"a", 1})
        {from, join} = next_heartbeat()
        reply_ok(from, join, 1, assignment([{@id_a, "a", [0, 1]}]))
        assert_receive {:started, p0, {"a", 0}, _}
        assert_receive {:init_blocked, p1, {"a", 1}}
        :ok = await_owned(manager, p0)

        %{op: %{kind: :assign, pid: worker, ref: op_ref}} = :sys.get_state(manager)
        Process.exit(worker, :kill)
        :ok = await_state(manager, &(not match?(%{ref: ^op_ref}, &1.op)))

        # Owned once, monitored once.
        state = :sys.get_state(manager)
        assert %{status: :running, pid: ^p0, ref: p0_ref} = state.owned[{@id_a, 0}]
        assert {:monitored_by, monitors} = Process.info(p0, :monitored_by)
        assert Enum.count(monitors, &(&1 == manager)) == 1
        assert {:monitors, manager_monitors} = Process.info(manager, :monitors)
        assert Enum.count(manager_monitors, &(&1 == {:process, p0})) == 1
        assert is_reference(p0_ref)

        send(p1, :release_init)
        assert_receive {:started, ^p1, {"a", 1}, _}
      end)
    end

    test "a consumer that stops normally is not restarted" do
      quiet(fn ->
        %{manager: manager} = start_group()
        {_member_id, %{0 => pid}} = join_with([0])
        :ok = GenServer.call(pid, {:exit, :normal})
        refute_receive {:started, _, {"a", 0}, _}, 200
        assert Process.alive?(manager)
      end)
    end
  end

  # --- terminal errors --------------------------------------------------------

  describe "terminal heartbeat errors" do
    for reason <- [
          :group_authorization_failed,
          :unsupported_assignor,
          :unreleased_instance_id,
          :fenced_instance_id,
          :group_max_size_reached,
          :invalid_regular_expression,
          :unsupported_version,
          :invalid_request,
          :api_not_supported_by_broker
        ] do
      test "#{reason} stops the group with a clear reason, without commits or a leave" do
        reason = unquote(reason)
        attach_telemetry([[:kafka_ex, :consumer, :member_terminated]])

        quiet(fn ->
          %{sup: sup, manager: manager} = start_group()
          sup_ref = Process.monitor(sup)
          manager_ref = Process.monitor(manager)
          {_member_id, %{0 => pid}} = join_with([0])

          {from, _} = poke(manager)
          GenServer.reply(from, {:error, reason})

          assert_receive {:DOWN, ^manager_ref, :process, ^manager, {:shutdown, {:terminal, ^reason}}}, 5_000
          assert_receive {:terminated, ^pid, {"a", 0}, {:shutdown, :partition_lost}}
          assert_receive {:telemetry, [:kafka_ex, :consumer, :member_terminated], _, %{reason: ^reason}}
          assert_receive {:DOWN, ^sup_ref, :process, ^sup, _}, 5_000
          refute_received {:heartbeat_call, _, _, _}
        end)
      end
    end

    test "a terminal error on the join stops the member too" do
      quiet(fn ->
        %{sup: sup, manager: manager} = start_group()
        ref = Process.monitor(manager)
        sup_ref = Process.monitor(sup)
        {from, _} = next_heartbeat()
        GenServer.reply(from, {:error, :group_authorization_failed})
        assert_receive {:DOWN, ^ref, :process, ^manager, {:shutdown, {:terminal, :group_authorization_failed}}}
        assert_receive {:DOWN, ^sup_ref, :process, ^sup, _}, 5_000
      end)
    end
  end

  # --- leave ------------------------------------------------------------------

  describe "leave" do
    test "on shutdown the consumers stop (and commit) first, then epoch -1 is sent" do
      attach_telemetry([[:kafka_ex, :consumer, :member_left]])
      %{sup: sup} = start_group()
      {member_id, pids} = join_with([0, 1])

      stop_async(sup)

      for p <- [0, 1] do
        pid = pids[p]
        assert_receive {:terminated, ^pid, {"a", ^p}, :shutdown}
      end

      {from, leave} = next_heartbeat()
      assert leave[:member_epoch] == -1
      assert leave[:member_id] == member_id
      reply_ok(from, leave, -1)

      assert_receive {:telemetry, [:kafka_ex, :consumer, :member_left], %{count: 1}, meta}
      assert meta.member_epoch == -1
      assert meta.result == :ok
      assert_receive {:sup_down, :shutdown}, 5_000
    end

    test "a static member leaves with epoch -2 and its instance id" do
      %{sup: sup} = start_group(group_instance_id: "inst-1")
      join_with([0])
      stop_async(sup)
      {from, leave} = next_heartbeat()
      assert leave[:member_epoch] == -2
      assert leave[:instance_id] == "inst-1"
      reply_ok(from, leave, -2)
      assert_receive {:sup_down, :shutdown}, 5_000
    end

    test "a leave that gets no answer is bounded by :leave_timeout_ms" do
      quiet(fn ->
        %{sup: sup} = start_group(leave_timeout_ms: 200)
        join_with([0])
        started = System.monotonic_time(:millisecond)
        stop_async(sup)
        {_from, leave} = next_heartbeat()
        assert leave[:member_epoch] == -1
        assert_receive {:sup_down, :shutdown}, 5_000
        assert System.monotonic_time(:millisecond) - started < 2_000
      end)
    end

    # Java leaveGroup leaves unless FENCED / FATAL: the broker may have
    # registered the member from a join whose answer never arrived.
    test "a member stopped while its join is unanswered still leaves (epoch -1, same member id)" do
      %{sup: sup} = start_group()
      {_from, join} = next_heartbeat()
      assert join[:member_epoch] == 0

      stop_async(sup)
      {from, leave} = next_heartbeat()
      assert leave[:member_epoch] == -1
      assert leave[:member_id] == join[:member_id]
      reply_ok(from, leave, -1)
      assert_receive {:sup_down, :shutdown}, 5_000
    end

    test "a static member stopped while joining leaves with -2, so a restart is not rejected" do
      %{sup: sup} = start_group(group_instance_id: "inst-1")
      {_from, join} = next_heartbeat()
      stop_async(sup)
      {from, leave} = next_heartbeat()
      assert leave[:member_epoch] == -2
      assert leave[:member_id] == join[:member_id]
      assert leave[:instance_id] == "inst-1"
      reply_ok(from, leave, -2)
      assert_receive {:sup_down, :shutdown}, 5_000
    end

    test "a member stopped while rejoining after a fence leaves too" do
      quiet(fn ->
        %{manager: manager, sup: sup} = start_group()
        {member_id, %{0 => p0}} = join_with([0])
        {from, _} = poke(manager)
        GenServer.reply(from, {:error, :fenced_member_epoch})
        assert_receive {:terminated, ^p0, _, {:shutdown, :partition_lost}}

        {_from, rejoin} = next_heartbeat()
        assert rejoin[:member_epoch] == 0
        stop_async(sup)
        {from, leave} = next_heartbeat()
        assert leave[:member_epoch] == -1
        assert leave[:member_id] == member_id
        reply_ok(from, leave, -1)
        assert_receive {:sup_down, :shutdown}, 5_000
      end)
    end

    test "a member stopped after a fence but before its rejoin is sent sends no leave" do
      attach_telemetry([[:kafka_ex, :consumer, :member_left]])

      quiet(fn ->
        %{manager: manager} = start_group()
        {_member_id, %{0 => p0}} = join_with([0])
        :ok = GenServer.call(p0, :block_stop)
        {from, _} = poke(manager)
        GenServer.reply(from, {:error, :fenced_member_epoch})
        assert_receive {:stop_blocked, ^p0, _}

        # A heartbeat (a stand-in that never answers) is in flight, so the
        # rejoin that follows the release of the partitions is held back.
        :sys.replace_state(manager, fn state ->
          {pid, ref} = spawn_monitor(fn -> Process.sleep(:infinity) end)
          %{state | inflight: %{pid: pid, ref: ref, partitions: nil, epoch: 1}}
        end)

        send(p0, :release_stop)
        assert_receive {:terminated, ^p0, {"a", 0}, {:shutdown, :partition_lost}}
        :ok = await_state(manager, &(&1.phase == :joining and &1.heartbeat_pending))

        ref = Process.monitor(manager)
        spawn(fn -> GenServer.stop(manager, :shutdown, 10_000) end)
        assert_receive {:DOWN, ^ref, :process, ^manager, :shutdown}, 5_000
        refute_received {:heartbeat_call, _, _, _}
        refute_received {:telemetry, [:kafka_ex, :consumer, :member_left], _, _}
      end)
    end

    test "a member stopped while fenced (releasing its partitions) sends no leave, as in Java" do
      quiet(fn ->
        %{manager: manager} = start_group()
        {_member_id, %{0 => p0}} = join_with([0])
        :ok = GenServer.call(p0, :block_stop)
        {from, _} = poke(manager)
        GenServer.reply(from, {:error, :fenced_member_epoch})
        assert_receive {:stop_blocked, ^p0, _}

        ref = Process.monitor(manager)
        spawn(fn -> GenServer.stop(manager, :shutdown, 10_000) end)
        send(p0, :release_stop)
        assert_receive {:DOWN, ^ref, :process, ^manager, :shutdown}, 5_000
        refute_received {:heartbeat_call, _, _, _}
      end)
    end

    test "the manager's shutdown covers the consumers' stop and the leave" do
      budget = ConsumerProtocolManager.shutdown_budget(fetch_options: [max_wait_time: 1_000], leave_timeout_ms: 2_000)
      assert budget >= 1_000 + 10_000 + 2_000

      assert %{shutdown: shutdown} =
               ConsumerProtocolManager.child_spec({{FakeConsumer, NoopConsumer}, @group, ["a"], [shutdown: 3_000]})

      assert shutdown >= 3_000 + 5_000
    end
  end

  # --- introspection ------------------------------------------------------------

  test "ConsumerGroup introspection works on a KIP-848 member" do
    %{sup: sup} = start_group()
    {member_id, pids} = join_with([0, 1])

    assert ConsumerGroup.member_id(sup) == member_id
    assert ConsumerGroup.generation_id(sup) == 1
    assert ConsumerGroup.assignments(sup) == [{"a", 0}, {"a", 1}]
    assert ConsumerGroup.leader?(sup) == false
    assert is_pid(consumer_sup = ConsumerGroup.consumer_supervisor_pid(sup))
    assert GenConsumerSupervisor.partition_pids(consumer_sup) == %{{"a", 0} => pids[0], {"a", 1} => pids[1]}
    assert ConsumerGroup.active?(sup)
  end
end
