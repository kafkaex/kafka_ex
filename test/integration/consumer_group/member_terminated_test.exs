defmodule KafkaEx.Integration.ConsumerGroup.MemberTerminatedTest do
  @moduledoc """
  Exercises the terminal/permanent-death path against the shared integration
  cluster (start it with `./scripts/docker_up.sh`).

  A real broker terminal (fenced static instance id, group-auth failure) needs
  static-membership or ACL infrastructure the test cluster lacks, so we force a
  deterministic NON-recoverable death instead: corrupt the live Heartbeat's
  `client` to a dead pid, so its next `KafkaExAPI.heartbeat/4` call exits
  `:noproc` → `{:shutdown, {:error, :noproc}}` → the Manager stops terminally.

  Asserts the new `[:kafka_ex, :consumer, :member_terminated]` telemetry fires
  (so a permanent death is alertable, not aliased onto a graceful leave) and the
  one_for_all / max_restarts: 0 member subtree is torn down.
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

    topic = "cg_member_terminated_#{:rand.uniform(1_000_000)}"
    create_topic(client, topic, partitions: 1)
    wait_for_topic_in_metadata(client, topic)

    {:ok, %{uris: uris, topic: topic}}
  end

  test "a non-recoverable heartbeat failure stops the member and emits member_terminated",
       %{uris: uris, topic: topic} do
    Process.flag(:trap_exit, true)
    group = "cg_member_terminated_grp_#{:rand.uniform(1_000_000)}"

    test_pid = self()
    handler_id = {__MODULE__, make_ref()}

    :telemetry.attach(
      handler_id,
      [:kafka_ex, :consumer, :member_terminated],
      fn _event, _measurements, %{group_id: ^group} = meta, _ -> send(test_pid, {:member_terminated, meta}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    opts = [
      uris: uris,
      heartbeat_interval: 1_000,
      session_timeout: 30_000,
      commit_interval: 1_000,
      auto_offset_reset: :earliest,
      extra_consumer_args: %{test_pid: self()}
    ]

    {:ok, cg} = ConsumerGroup.start_link(TestGenConsumer, group, [topic], opts)
    on_exit(fn -> ConsumerGroupHelpers.stop_consumer_group(cg) end)

    assert {:ok, :active} = ConsumerGroupHelpers.wait_for_active(cg, timeout: 60_000)
    assert {:ok, _} = ConsumerGroupHelpers.wait_for_assignments(cg, timeout: 30_000)

    # Point the live Heartbeat at a dead client so its next call exits :noproc —
    # a non-recoverable error the Manager turns into a terminal stop.
    {pid, ref} = spawn_monitor(fn -> :ok end)

    receive do
      {:DOWN, ^ref, :process, ^pid, _} -> :ok
    end

    manager = ConsumerGroup.get_manager_pid(cg)
    heartbeat = :sys.get_state(manager).heartbeat_timer
    :sys.replace_state(heartbeat, fn hb_state -> %{hb_state | client: pid} end)

    # member_terminated fires with a non-recoverable {:error, _} reason...
    assert_receive {:member_terminated, %{reason: {:error, _}, member_id: member_id}}, 30_000
    assert is_binary(member_id)

    # ...and the member subtree is torn down (max_restarts: 0, no restart).
    ref = Process.monitor(cg)
    assert_receive {:DOWN, ^ref, :process, ^cg, _}, 30_000
    refute Process.alive?(cg)
  end
end
