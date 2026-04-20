defmodule KafkaEx.Consumer.GenConsumerCommitTest do
  @moduledoc """
  Unit tests for GenConsumer's commit-error handling path.

  Exercises the test-only `commit_for_test/2` seam that wraps
  `handle_commit_error/2`. Four distinct classes of behaviour:

    * **Terminal** (`:fenced_instance_id`) — KIP-345. Stop consumer without
      rejoin cast; another member owns the group.instance.id.
    * **Fatal** (`:illegal_generation` / `:unknown_member_id` /
      `:rebalance_in_progress`) — cast to group manager with stale
      generation_id tag, then self-stop.
    * **Fatal with no manager pid** — log + stop, no cast.
    * **Transient** — log + noreply, state unchanged.
  """
  # async: false — handle_commit_error writes Logger.error messages that can
  # leak into other modules' capture_log assertions under concurrent runs.
  use ExUnit.Case, async: false
  import ExUnit.CaptureLog
  alias KafkaEx.Consumer.GenConsumer

  defmodule FakeManager do
    @moduledoc false
    use GenServer
    def start_link(test_pid), do: GenServer.start_link(__MODULE__, test_pid)
    @impl true
    def init(test_pid), do: {:ok, test_pid}
    @impl true
    def handle_cast({:rejoin_required, _, _} = msg, test_pid) do
      send(test_pid, {:manager_received, msg})
      {:noreply, test_pid}
    end
  end

  defp build_state(manager_pid) do
    %GenConsumer.State{
      client: self(),
      group: "test-group",
      topic: "t",
      partition: 0,
      member_id: "m",
      generation_id: 7,
      acked_offset: 42,
      committed_offset: 0,
      last_commit: 0,
      group_manager_pid: manager_pid,
      api_versions: %{}
    }
  end

  describe "fatal errors (rejoinable)" do
    for fatal_error <- [:illegal_generation, :unknown_member_id] do
      test "#{inspect(fatal_error)} casts {:rejoin_required, reason, stale_gen} and stops" do
        fatal_error = unquote(fatal_error)
        {:ok, manager} = FakeManager.start_link(self())
        state = build_state(manager)

        log =
          capture_log(fn ->
            send(self(), {:result, GenConsumer.commit_for_test(fatal_error, state)})
          end)

        assert_received {:result, {:stop, {:shutdown, {:rejoin_required, ^fatal_error}}, _}}
        assert_receive {:manager_received, {:rejoin_required, ^fatal_error, 7}}, 500
        assert log =~ "Commit fatal error"
        assert log =~ "stale_gen=7"
      end
    end

    test "fatal error with nil manager pid: stops without cast, logs error" do
      state = build_state(nil)

      log =
        capture_log(fn ->
          result = GenConsumer.commit_for_test(:illegal_generation, state)
          assert {:stop, {:shutdown, {:rejoin_required, :illegal_generation}}, _} = result
        end)

      refute_receive {:manager_received, _}, 100
      assert log =~ "no group_manager_pid"
    end

    test "dead manager pid: cast is silent no-op, consumer still stops cleanly" do
      {:ok, manager} = FakeManager.start_link(self())
      ref = Process.monitor(manager)
      GenServer.stop(manager)
      assert_receive {:DOWN, ^ref, :process, _, _}

      state = build_state(manager)

      capture_log(fn ->
        result = GenConsumer.commit_for_test(:illegal_generation, state)
        assert {:stop, {:shutdown, {:rejoin_required, :illegal_generation}}, _} = result
      end)
    end
  end

  describe "terminal errors (non-rejoinable per KIP-345)" do
    test ":fenced_instance_id stops without casting to manager" do
      {:ok, manager} = FakeManager.start_link(self())
      state = build_state(manager)

      log =
        capture_log(fn ->
          result = GenConsumer.commit_for_test(:fenced_instance_id, state)
          assert {:stop, {:shutdown, {:terminal_error, :fenced_instance_id}}, _} = result
        end)

      refute_receive {:manager_received, _}, 100
      assert log =~ "terminal error"
      assert log =~ "another member owns"
    end
  end

  describe "transient errors" do
    test ":coordinator_not_available: noreply, no cast, state unchanged" do
      {:ok, manager} = FakeManager.start_link(self())
      state = build_state(manager)

      log =
        capture_log(fn ->
          result = GenConsumer.commit_for_test(:coordinator_not_available, state)
          assert {:noreply, ^state} = result
        end)

      refute_receive {:manager_received, _}, 100
      assert log =~ "Failed to commit offset"
    end
  end
end
