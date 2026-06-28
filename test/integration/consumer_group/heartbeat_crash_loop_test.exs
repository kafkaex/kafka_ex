defmodule KafkaEx.Integration.ConsumerGroup.HeartbeatCrashLoopTest do
  @moduledoc """
  Exercises the heartbeat-crash-loop bound against the shared integration cluster
  (start it with `./scripts/docker_up.sh`).

  Repeatedly hard-kills the live Heartbeat (Process.exit/2 :kill, an abnormal
  reason heartbeat.ex cannot catch). Each kill drives an in-place rejoin until the
  crash count exceeds `crash_rejoin_max_restarts` within `crash_rejoin_window_ms`,
  at which point the member STOPS terminally (emitting member_terminated with a
  {:crash_loop, _} reason) and is torn down by the one_for_all / max_restarts: 0
  supervisor — proving the loop is bounded, not infinite.
  """
  use ExUnit.Case, async: false

  @moduletag :consumer_group
  @moduletag timeout: 120_000

  import KafkaEx.IntegrationHelpers

  alias KafkaEx.Client
  alias KafkaEx.Consumer.ConsumerGroup
  alias KafkaEx.TestSupport.ConsumerGroupHelpers
  alias KafkaEx.TestSupport.ProcessHelpers
  alias KafkaEx.TestSupport.TestGenConsumer

  setup do
    {:ok, args} = KafkaEx.build_worker_options([])
    uris = Keyword.fetch!(args, :uris)

    {:ok, client} = Client.start_link(args, :no_name)
    on_exit(fn -> ProcessHelpers.stop_safely(client) end)

    topic = "cg_hb_crash_loop_#{:rand.uniform(1_000_000)}"
    create_topic(client, topic, partitions: 1)
    wait_for_topic_in_metadata(client, topic)

    {:ok, %{uris: uris, topic: topic}}
  end

  test "repeated heartbeat kills past the budget stop the member and emit member_terminated",
       %{uris: uris, topic: topic} do
    Process.flag(:trap_exit, true)
    group = "cg_hb_crash_loop_grp_#{:rand.uniform(1_000_000)}"

    test_pid = self()
    handler_id = {__MODULE__, make_ref()}

    :telemetry.attach(
      handler_id,
      [:kafka_ex, :consumer, :member_terminated],
      fn _event, _measurements, %{group_id: ^group} = meta, _ -> send(test_pid, {:member_terminated, meta}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    max_restarts = 2

    opts = [
      uris: uris,
      heartbeat_interval: 1_000,
      session_timeout: 30_000,
      commit_interval: 1_000,
      auto_offset_reset: :earliest,
      crash_rejoin_max_restarts: max_restarts,
      crash_rejoin_window_ms: 60_000,
      crash_rejoin_max_jitter_ms: 0,
      extra_consumer_args: %{test_pid: self()}
    ]

    {:ok, cg} = ConsumerGroup.start_link(TestGenConsumer, group, [topic], opts)
    on_exit(fn -> ConsumerGroupHelpers.stop_consumer_group(cg) end)

    assert {:ok, :active} = ConsumerGroupHelpers.wait_for_active(cg, timeout: 60_000)
    assert {:ok, _} = ConsumerGroupHelpers.wait_for_assignments(cg, timeout: 30_000)

    cg_ref = Process.monitor(cg)

    # Hard-kill the live heartbeat repeatedly until the member trips terminal.
    # Each kill drives an in-place rejoin (a fresh heartbeat pid); once crashes
    # exceed the budget within the window the member stops and the cg goes DOWN.
    kill_until_down(cg)

    assert_receive {:member_terminated, %{reason: {:crash_loop, _}, member_id: member_id}}, 60_000
    assert is_binary(member_id)

    assert_receive {:DOWN, ^cg_ref, :process, ^cg, _}, 60_000
    refute Process.alive?(cg)
  end

  # Generous anti-hang backstop; the real timeout is the test's assert_receive.
  @kill_cap 600

  # Repeatedly hard-kill the live heartbeat until the member trips terminal (the
  # cg dies). Robust to the Manager being mid-rebalance: a transient nil/dead
  # heartbeat (e.g. heartbeat_timer not yet swapped) is retried, not treated as
  # "done", so the loop never ends early before the budget is exceeded. Stops on
  # `Process.alive?(cg)` rather than receiving the `:DOWN` — leaving that message
  # in the mailbox for the test's own assert_receive.
  defp kill_until_down(cg, cap \\ @kill_cap)
  defp kill_until_down(_cg, 0), do: :timeout

  defp kill_until_down(cg, cap) do
    if Process.alive?(cg) do
      case live_heartbeat(cg) do
        nil -> Process.sleep(50)
        hb -> Process.exit(hb, :kill)
      end

      kill_until_down(cg, cap - 1)
    else
      :down
    end
  end

  defp live_heartbeat(cg) do
    case current_heartbeat(cg) do
      hb when is_pid(hb) -> if Process.alive?(hb), do: hb, else: nil
      _ -> nil
    end
  end

  # NOTE: :sys.get_state has a 5s default timeout. While the Manager is mid-rebalance
  # (a synchronous join/sync chain) it won't reply until that returns; we absorb the
  # timeout/exit as nil and let the caller retry. Self-correcting, not a hard failure.
  defp current_heartbeat(cg) do
    case ConsumerGroup.get_manager_pid(cg) do
      pid when is_pid(pid) ->
        if Process.alive?(pid), do: :sys.get_state(pid).heartbeat_timer, else: nil

      _ ->
        nil
    end
  rescue
    _ -> nil
  catch
    :exit, _ -> nil
  end
end
