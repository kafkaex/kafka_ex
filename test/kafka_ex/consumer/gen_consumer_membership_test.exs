defmodule KafkaEx.Consumer.GenConsumerMembershipTest do
  @moduledoc """
  `GenConsumer.update_membership/3` and the membership each commit carries.

  A classic member commits with `generation_id`; a KIP-848 member commits with
  `member_epoch` (OffsetCommit v9+ `generation_id_or_member_epoch`) and must
  use the latest epoch it was given, without being restarted.

  Driven through the real consume loop against `KafkaEx.Test.MockClient`, so the
  asserted options are the ones `KafkaEx.API.commit_offset/5` /
  `fetch_committed_offset/5` really receive.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog
  import KafkaEx.TestSupport.ProcessHelpers

  alias KafkaEx.Consumer.GenConsumer
  alias KafkaEx.Messages.Fetch
  alias KafkaEx.Messages.Fetch.Record
  alias KafkaEx.Test.MockClient

  defmodule TestConsumer do
    use KafkaEx.Consumer.GenConsumer
    def handle_message_set(_messages, state), do: {:sync_commit, state}
  end

  # Never asks for a commit itself: with a high threshold/interval only
  # terminate/2 commits.
  defmodule AsyncConsumer do
    use KafkaEx.Consumer.GenConsumer
    def handle_message_set(_messages, state), do: {:async_commit, state}
  end

  defmodule FakeManager do
    use GenServer
    def start_link(test_pid), do: GenServer.start_link(__MODULE__, test_pid)
    def init(test_pid), do: {:ok, test_pid}

    def handle_cast(msg, test_pid) do
      send(test_pid, {:manager_received, msg})
      {:noreply, test_pid}
    end
  end

  # A MockClient whose fetch returns one record at the requested offset, so
  # every consume cycle advances and produces a :sync_commit of a new offset.
  # Everything else is MockClient (calls recorded, put_response/3).
  defmodule AdvancingClient do
    use GenServer

    def start_link(responses), do: GenServer.start_link(__MODULE__, responses)

    def init(responses) do
      {:ok, mock} = MockClient.start_link(responses)
      {:ok, mock}
    end

    def handle_call({:fetch, _topic, _partition, offset, _opts} = msg, _from, mock) do
      _ = GenServer.call(mock, msg)
      Process.sleep(2)
      {:reply, {:ok, %Fetch{records: [%Record{offset: offset, value: "v"}]}}, mock}
    end

    # A scripted {:block, test_pid} commit response: record the call, then
    # hold the reply until the test sends :release_commit (an in-flight
    # commit). The released commit replies {:ok, []}.
    def handle_call({:offset_commit, _, _, _} = msg, _from, mock) do
      case GenServer.call(mock, msg) do
        {:block, test_pid} ->
          send(test_pid, {:commit_blocked, self(), mock})

          receive do
            :release_commit -> {:reply, {:ok, []}, mock}
          end

        reply ->
          {:reply, reply, mock}
      end
    end

    def handle_call(msg, _from, mock), do: {:reply, GenServer.call(mock, msg), mock}
  end

  defp start_consumer(opts, responses \\ %{}, consumer_module \\ TestConsumer) do
    {:ok, client} =
      AdvancingClient.start_link(
        Map.merge(
          %{
            offset_fetch: {:ok, [%{partition_offsets: [%{offset: 0, error_code: :no_error}]}]},
            offset_commit: {:ok, []}
          },
          responses
        )
      )

    Process.unlink(client)

    {:ok, pid} = GenServer.start_link(GenConsumer, {consumer_module, "g", "t", 0, [client: client] ++ opts})
    Process.unlink(pid)

    on_exit(fn ->
      stop_safely(pid)
      stop_safely(client)
    end)

    {client, pid}
  end

  defp commit_opts(client) do
    for {:offset_commit, "g", [{"t", _}], opts} <- MockClient.get_calls(client), do: opts
  end

  defp fetch_calls(client) do
    for {:fetch, "t", 0, _, _} = call <- MockClient.get_calls(client), do: call
  end

  defp offset_fetch_opts(client) do
    for {:offset_fetch, "g", [{"t", _}], opts} <- MockClient.get_calls(client), do: opts
  end

  defp wait_until(fun, deadline \\ nil) do
    deadline = deadline || System.monotonic_time(:millisecond) + 2_000

    cond do
      fun.() ->
        :ok

      System.monotonic_time(:millisecond) > deadline ->
        flunk("condition not met in time")

      true ->
        Process.sleep(5)
        wait_until(fun, deadline)
    end
  end

  defp wait_for_commits(client, count, deadline \\ nil) do
    deadline = deadline || System.monotonic_time(:millisecond) + 2_000
    opts = commit_opts(client)

    cond do
      length(opts) >= count ->
        opts

      System.monotonic_time(:millisecond) > deadline ->
        flunk("expected #{count} commits, got #{length(opts)}")

      true ->
        Process.sleep(5)
        wait_for_commits(client, count, deadline)
    end
  end

  describe "classic membership (no member epoch)" do
    test "commits with generation_id and member_id, no member_epoch" do
      {client, _pid} = start_consumer(member_id: "m", generation_id: 7)

      [opts | _] = wait_for_commits(client, 1)
      assert opts[:generation_id] == 7
      assert opts[:member_id] == "m"
      refute Keyword.has_key?(opts, :member_epoch)
    end

    test "loads offsets without member identity" do
      {client, _pid} = start_consumer(member_id: "m", generation_id: 7)
      wait_for_commits(client, 1)

      [{:offset_fetch, "g", _, fetch_opts} | _] =
        for {:offset_fetch, _, _, _} = call <- MockClient.get_calls(client), do: call

      refute Keyword.has_key?(fetch_opts, :member_epoch)
      refute Keyword.has_key?(fetch_opts, :member_id)
    end

    test "commits are byte-identical to master: exactly member_id and generation_id" do
      {client, _pid} = start_consumer(member_id: "m", generation_id: 7)
      [opts | _] = wait_for_commits(client, 1)
      assert opts == [member_id: "m", generation_id: 7]
      assert [[] | _] = offset_fetch_opts(client)
    end
  end

  describe "KIP-848 membership (member epoch)" do
    test "commits with member_epoch instead of generation_id" do
      {client, _pid} = start_consumer(member_id: "m", member_epoch: 3)

      [opts | _] = wait_for_commits(client, 1)
      assert opts[:member_epoch] == 3
      assert opts[:member_id] == "m"
      refute Keyword.has_key?(opts, :generation_id)
    end

    test "loads offsets with member_id and member_epoch" do
      {client, _pid} = start_consumer(member_id: "m", member_epoch: 3)
      wait_for_commits(client, 1)

      [{:offset_fetch, "g", _, fetch_opts} | _] =
        for {:offset_fetch, _, _, _} = call <- MockClient.get_calls(client), do: call

      assert fetch_opts[:member_epoch] == 3
      assert fetch_opts[:member_id] == "m"
    end

    test "every commit after update_membership/3 returns uses the new epoch, without a restart" do
      {client, pid} = start_consumer(member_id: "m", member_epoch: 3)
      wait_for_commits(client, 1)

      assert :ok = GenConsumer.update_membership(pid, member_epoch: 4)
      # The call is acked only once the state holds epoch 4, so every commit
      # recorded after this point was built with it.
      after_update = length(commit_opts(client))
      new_commits = client |> wait_for_commits(after_update + 3) |> Enum.drop(after_update)

      assert Enum.all?(new_commits, &(&1[:member_epoch] == 4))
      assert Enum.all?(new_commits, &(&1[:member_id] == "m"))
      assert Process.alive?(pid)
    end

    test "a {:shutdown, :partition_lost} stop skips the final commit" do
      {client, pid} =
        start_consumer(
          [member_id: "m", member_epoch: 3, commit_threshold: 1_000_000, commit_interval: 600_000],
          %{},
          AsyncConsumer
        )

      wait_until(fn -> length(fetch_calls(client)) >= 3 end)
      assert :ok = GenServer.stop(pid, {:shutdown, :partition_lost})
      assert commit_opts(client) == []
    end

    test "a :shutdown stop still makes the final commit" do
      {client, pid} =
        start_consumer(
          [member_id: "m", member_epoch: 3, commit_threshold: 1_000_000, commit_interval: 600_000],
          %{},
          AsyncConsumer
        )

      wait_until(fn -> length(fetch_calls(client)) >= 3 end)
      assert :ok = GenServer.stop(pid, :shutdown)
      assert [_] = commit_opts(client)
    end

    test "update_membership/3 also updates the commit made on shutdown" do
      # :async_commit with a threshold/interval that never trigger, so the
      # only commit is the one terminate/2 makes.
      {client, pid} =
        start_consumer(
          [member_id: "m", member_epoch: 3, commit_threshold: 1_000_000, commit_interval: 600_000],
          %{},
          AsyncConsumer
        )

      wait_until(fn -> Enum.count(MockClient.get_calls(client), &match?({:fetch, _, _, _, _}, &1)) >= 3 end)
      :ok = GenConsumer.update_membership(pid, member_id: "m2", member_epoch: 9)
      wait_until(fn -> Enum.count(MockClient.get_calls(client), &match?({:fetch, _, _, _, _}, &1)) >= 6 end)
      assert commit_opts(client) == []

      stop_safely(pid)

      assert [opts] = commit_opts(client)
      assert opts[:member_epoch] == 9
      assert opts[:member_id] == "m2"
      refute Keyword.has_key?(opts, :generation_id)
    end

    test "update_membership/3 can change the member id too" do
      {client, pid} = start_consumer(member_id: "m", member_epoch: 3)
      wait_for_commits(client, 1)

      :ok = GenConsumer.update_membership(pid, member_id: "m2", member_epoch: 4)
      before = length(commit_opts(client))
      opts = client |> wait_for_commits(before + 1) |> List.last()

      assert opts == [member_id: "m2", member_epoch: 4]
    end

    test "a commit already in flight keeps the epoch it was built with" do
      {client, pid} = start_consumer([member_id: "m", member_epoch: 3], %{offset_commit: {:block, self()}})
      assert_receive {:commit_blocked, wrapper, mock}, 2_000

      task = Task.async(fn -> GenConsumer.update_membership(pid, member_epoch: 4) end)
      # The update is queued behind the in-flight commit, not applied in it.
      wait_until(fn -> match?({:messages, [_ | _]}, Process.info(pid, :messages)) end)
      refute Task.yield(task, 50)

      # The wrapper is busy holding the commit; script the inner mock.
      :ok = MockClient.put_response(mock, :offset_commit, {:ok, []})
      send(wrapper, :release_commit)
      assert Task.await(task) == :ok

      [first | rest] = wait_for_commits(client, 2)
      assert first[:member_epoch] == 3
      assert Enum.all?(rest, &(&1[:member_epoch] == 4))
    end
  end

  describe "offset fetch at startup with a stale member epoch" do
    setup do
      # Long enough that only update_membership/3 can trigger the retry inside
      # the test's time budget.
      original = Application.get_env(:kafka_ex, :load_offsets_retry_backoff_ms)
      Application.put_env(:kafka_ex, :load_offsets_retry_backoff_ms, 60_000)

      on_exit(fn ->
        if is_nil(original),
          do: Application.delete_env(:kafka_ex, :load_offsets_retry_backoff_ms),
          else: Application.put_env(:kafka_ex, :load_offsets_retry_backoff_ms, original)
      end)
    end

    test "the consumer stays up and loads offsets with the epoch update_membership/3 gives it" do
      log =
        capture_log(fn ->
          {client, pid} =
            start_consumer([member_id: "m", member_epoch: 3], %{offset_fetch: {:error, :stale_member_epoch}})

          ref = Process.monitor(pid)
          wait_until(fn -> length(offset_fetch_opts(client)) >= 1 end)
          refute_receive {:DOWN, ^ref, :process, ^pid, _}, 100
          assert length(offset_fetch_opts(client)) == 1
          assert commit_opts(client) == []

          :ok =
            MockClient.put_response(
              client,
              :offset_fetch,
              {:ok, [%{partition_offsets: [%{offset: 0, error_code: :no_error}]}]}
            )

          :ok = GenConsumer.update_membership(pid, member_epoch: 4)

          assert List.last(wait_for_commits(client, 1))[:member_epoch] == 4
          assert [first, second | _] = offset_fetch_opts(client)
          assert first[:member_epoch] == 3
          assert second[:member_epoch] == 4
          assert Process.alive?(pid)
        end)

      assert log =~ "stale_member_epoch"
    end

    test "other messages to a waiting consumer do not trigger an early refetch" do
      capture_log(fn ->
        {client, pid} =
          start_consumer([member_id: "m", member_epoch: 3], %{offset_fetch: {:error, :stale_member_epoch}})

        wait_until(fn -> length(offset_fetch_opts(client)) >= 1 end)

        for _ <- 1..5, do: assert(GenConsumer.partition(pid) == {"t", 0})
        send(pid, :unrelated)
        Process.sleep(50)

        assert length(offset_fetch_opts(client)) == 1
        assert Process.alive?(pid)
      end)
    end

    test "retries back off exponentially and only the first rejection is a warning" do
      Application.put_env(:kafka_ex, :load_offsets_retry_backoff_ms, 100)
      assert Enum.map(1..4, &GenConsumer.stale_epoch_backoff_ms/1) == [100, 200, 400, 800]
      assert GenConsumer.stale_epoch_backoff_ms(100) == 30_000

      log =
        capture_log([level: :warning], fn ->
          {client, _pid} =
            start_consumer([member_id: "m", member_epoch: 3], %{offset_fetch: {:error, :stale_member_epoch}})

          wait_until(fn -> length(offset_fetch_opts(client)) >= 3 end)
        end)

      assert length(Regex.scan(~r/rejected with :stale_member_epoch/, log)) == 1
    end

    test "a :load_offsets_retry_backoff_ms of 0 still waits at least 100 ms between retries" do
      Application.put_env(:kafka_ex, :load_offsets_retry_backoff_ms, 0)
      assert Enum.map(1..3, &GenConsumer.stale_epoch_backoff_ms/1) == [100, 100, 100]

      capture_log(fn ->
        {client, pid} =
          start_consumer([member_id: "m", member_epoch: 3], %{offset_fetch: {:error, :stale_member_epoch}})

        Process.sleep(500)
        # The first fetch plus at most one retry per 100 ms.
        assert length(offset_fetch_opts(client)) in 2..6
        assert Process.alive?(pid)
      end)
    end

    for reason <- [:unknown_member_id, :fenced_member_epoch] do
      test "#{reason} tells the group manager and stops cleanly, without raising" do
        reason = unquote(reason)
        {:ok, manager} = FakeManager.start_link(self())

        log =
          capture_log(fn ->
            {client, pid} =
              start_consumer([member_id: "m", member_epoch: 3, group_manager_pid: manager], %{
                offset_fetch: {:error, reason}
              })

            ref = Process.monitor(pid)
            assert_receive {:DOWN, ^ref, :process, ^pid, {:shutdown, {:rejoin_required, ^reason}}}, 2_000
            assert length(offset_fetch_opts(client)) == 1
            assert commit_opts(client) == []
          end)

        assert_receive {:manager_received, {:rejoin_required, ^reason, 3}}
        refute log =~ "RuntimeError"
      end
    end

    test "unknown_member_id without a group manager still stops cleanly" do
      capture_log(fn ->
        {_client, pid} =
          start_consumer([member_id: "m", member_epoch: 3], %{offset_fetch: {:error, :unknown_member_id}})

        ref = Process.monitor(pid)
        assert_receive {:DOWN, ^ref, :process, ^pid, {:shutdown, {:rejoin_required, :unknown_member_id}}}, 2_000
      end)
    end

    test "without a member epoch (classic) it still fails to start" do
      Application.put_env(:kafka_ex, :load_offsets_retry_backoff_ms, 0)

      capture_log(fn ->
        {_client, pid} =
          start_consumer([member_id: "m", generation_id: 7], %{offset_fetch: {:error, :stale_member_epoch}})

        ref = Process.monitor(pid)
        assert_receive {:DOWN, ^ref, :process, ^pid, {%RuntimeError{message: message}, _}}, 2_000
        assert message =~ "Unable to load committed offsets"
      end)
    end
  end

  describe "commit errors with a member epoch" do
    test ":stale_member_epoch is transient: the consumer keeps the partition and recommits" do
      {:ok, manager} = FakeManager.start_link(self())

      log =
        capture_log(fn ->
          {client, pid} =
            start_consumer([member_id: "m", member_epoch: 3, group_manager_pid: manager], %{
              offset_commit: {:error, :stale_member_epoch}
            })

          wait_for_commits(client, 1)
          ref = Process.monitor(pid)
          refute_receive {:DOWN, ^ref, :process, ^pid, _}, 100

          # The group moves the member on; the next commit carries the new epoch
          # and succeeds.
          :ok = MockClient.put_response(client, :offset_commit, {:ok, []})
          :ok = GenConsumer.update_membership(pid, member_epoch: 4)
          before = length(commit_opts(client))
          assert List.last(wait_for_commits(client, before + 1))[:member_epoch] == 4
          assert Process.alive?(pid)
        end)

      refute_receive {:manager_received, {:rejoin_required, _, _}}, 50
      assert log =~ "stale_member_epoch"
    end

    # Resending with the same epoch fails the same way: after a rejection no
    # commit is sent for one commit interval, whatever triggers it (here a
    # fetch that returns a record every few ms and asks for a commit each
    # time), unless update_membership/3 arrives first.
    for {name, consumer, extra} <- [
          {":sync_commit", TestConsumer, []},
          {"the commit threshold", AsyncConsumer, [commit_threshold: 1]},
          {"the commit interval", AsyncConsumer, [commit_threshold: 1_000_000]}
        ] do
      test "after a :stale_member_epoch rejection, #{name} waits a commit interval before resending" do
        capture_log(fn ->
          {client, pid} =
            start_consumer(
              Keyword.merge(
                [member_id: "m", member_epoch: 3, commit_interval: 400],
                unquote(extra)
              ),
              %{offset_commit: {:error, :stale_member_epoch}},
              unquote(consumer)
            )

          wait_for_commits(client, 1)
          started = System.monotonic_time(:millisecond)
          fetches_before = length(fetch_calls(client))
          Process.sleep(250)

          # Records kept flowing, but no commit was resent.
          assert length(fetch_calls(client)) > fetches_before + 10
          assert length(commit_opts(client)) == 1

          # After the interval the acked offsets are committed again.
          wait_for_commits(client, 2)
          assert System.monotonic_time(:millisecond) - started >= 300
          assert Process.alive?(pid)
        end)
      end
    end

    test "update_membership/3 ends the wait after a :stale_member_epoch rejection at once" do
      capture_log(fn ->
        {client, pid} =
          start_consumer([member_id: "m", member_epoch: 3, commit_interval: 60_000], %{
            offset_commit: {:error, :stale_member_epoch}
          })

        wait_for_commits(client, 1)
        Process.sleep(50)
        assert length(commit_opts(client)) == 1

        :ok = MockClient.put_response(client, :offset_commit, {:ok, []})
        :ok = GenConsumer.update_membership(pid, member_epoch: 4)
        assert List.last(wait_for_commits(client, 2))[:member_epoch] == 4
      end)
    end

    test ":stale_member_epoch without a member epoch (classic) stays fatal" do
      {:ok, manager} = FakeManager.start_link(self())

      capture_log(fn ->
        {_client, pid} =
          start_consumer([member_id: "m", generation_id: 7, group_manager_pid: manager], %{
            offset_commit: {:error, :stale_member_epoch}
          })

        ref = Process.monitor(pid)
        assert_receive {:DOWN, ^ref, :process, ^pid, {:shutdown, {:rejoin_required, :stale_member_epoch}}}, 2_000
      end)

      assert_receive {:manager_received, {:rejoin_required, :stale_member_epoch, 7}}
    end

    test ":unknown_member_id is fatal and reports the epoch it was sent with" do
      {:ok, manager} = FakeManager.start_link(self())

      capture_log(fn ->
        {_client, pid} =
          start_consumer([member_id: "m", member_epoch: 3, group_manager_pid: manager], %{
            offset_commit: {:error, :unknown_member_id}
          })

        ref = Process.monitor(pid)
        assert_receive {:DOWN, ^ref, :process, ^pid, {:shutdown, {:rejoin_required, :unknown_member_id}}}, 2_000
      end)

      assert_receive {:manager_received, {:rejoin_required, :unknown_member_id, 3}}
    end

    test ":fenced_member_epoch is fatal and reports the epoch it was sent with" do
      {:ok, manager} = FakeManager.start_link(self())

      capture_log(fn ->
        {_client, pid} =
          start_consumer([member_id: "m", member_epoch: 3, generation_id: 1, group_manager_pid: manager], %{
            offset_commit: {:error, :fenced_member_epoch}
          })

        ref = Process.monitor(pid)
        assert_receive {:DOWN, ^ref, :process, ^pid, {:shutdown, {:rejoin_required, :fenced_member_epoch}}}, 2_000
      end)

      assert_receive {:manager_received, {:rejoin_required, :fenced_member_epoch, 3}}
    end
  end

  describe "commits with OffsetCommit pinned below v9" do
    test ":invalid_api_version stops the consumer as terminal, without a rejoin" do
      {:ok, manager} = FakeManager.start_link(self())

      log =
        capture_log(fn ->
          {_client, pid} =
            start_consumer([member_id: "m", member_epoch: 3, group_manager_pid: manager], %{
              offset_commit: {:error, :invalid_api_version}
            })

          ref = Process.monitor(pid)
          assert_receive {:DOWN, ^ref, :process, ^pid, {:shutdown, {:terminal_error, :invalid_api_version}}}, 2_000
        end)

      refute_receive {:manager_received, _}, 50
      assert log =~ "OffsetCommit v9+"
    end
  end

  describe "update_membership/3 errors" do
    test "a consumer that is not running is {:error, :noproc}, not an exit" do
      {_client, pid} = start_consumer(member_id: "m", member_epoch: 3)
      stop_safely(pid)
      refute Process.alive?(pid)

      assert GenConsumer.update_membership(pid, member_epoch: 4) == {:error, :noproc}
    end

    test "a consumer that does not reply in time is {:error, :timeout}, not an exit" do
      {client, pid} = start_consumer(member_id: "m", member_epoch: 3)
      wait_for_commits(client, 1)
      :ok = :sys.suspend(pid)

      assert GenConsumer.update_membership(pid, [member_epoch: 4], 50) == {:error, :timeout}

      :ok = :sys.resume(pid)
      assert Process.alive?(pid)
    end

    test "a consumer that exits during the call is {:error, :noproc}, not an exit" do
      {client, pid} = start_consumer(member_id: "m", member_epoch: 3)
      wait_for_commits(client, 1)
      :ok = :sys.suspend(pid)

      task = Task.async(fn -> GenConsumer.update_membership(pid, [member_epoch: 4], 5_000) end)
      wait_until(fn -> match?({:messages, [_ | _]}, Process.info(pid, :messages)) end)
      Process.exit(pid, :kill)

      assert Task.await(task) == {:error, :noproc}
    end
  end

  describe "start options" do
    test "a malformed :member_epoch or a missing :member_id is {:error, :invalid_membership}" do
      Process.flag(:trap_exit, true)
      {:ok, client} = MockClient.start_link(%{})

      for opts <- [
            [member_id: "m", member_epoch: -1],
            [member_id: "m", member_epoch: "3"],
            [member_id: "m", member_epoch: 2_147_483_648],
            [member_epoch: 3],
            [member_id: :m, member_epoch: 3]
          ] do
        assert GenConsumer.start_link(TestConsumer, "g", "t", 0, [client: client] ++ opts) ==
                 {:error, :invalid_membership},
               inspect(opts)
      end

      assert MockClient.get_calls(client) == []
    end
  end

  describe "update_membership/3 input" do
    test "rejects bad input without calling the consumer" do
      {_client, pid} = start_consumer(member_id: "m", member_epoch: 3)

      for bad <- [
            [member_epoch: "4"],
            [member_epoch: 2_147_483_648],
            [member_epoch: -1],
            [member_epoch: nil],
            [generation_id: 1],
            [member_id: :m],
            [member_id: nil],
            [bogus: 1],
            %{member_epoch: 4},
            :not_a_list
          ] do
        assert GenConsumer.update_membership(pid, bad) == {:error, :invalid_membership}, inspect(bad)
      end

      assert Process.alive?(pid)
    end

    test "a classic consumer (generation_id) is refused and stays classic" do
      {client, pid} = start_consumer(member_id: "m", generation_id: 7)
      wait_for_commits(client, 1)

      assert GenConsumer.update_membership(pid, member_epoch: 5) == {:error, :invalid_membership}
      assert GenConsumer.update_membership(pid, member_id: "m2", member_epoch: 5) == {:error, :invalid_membership}
      assert GenConsumer.update_membership(pid, member_id: "m2") == {:error, :invalid_membership}

      after_update = length(commit_opts(client))
      new_commits = client |> wait_for_commits(after_update + 2) |> Enum.drop(after_update)
      assert Enum.all?(new_commits, &(&1 == [member_id: "m", generation_id: 7]))
    end

    test "a consumer without membership never ends up with an epoch and no member id" do
      {client, pid} = start_consumer([])
      wait_for_commits(client, 1)

      assert GenConsumer.update_membership(pid, member_epoch: 5) == {:error, :invalid_membership}

      after_update = length(commit_opts(client))
      new_commits = client |> wait_for_commits(after_update + 2) |> Enum.drop(after_update)
      refute Enum.any?(new_commits, &Keyword.has_key?(&1, :member_epoch))
      assert :sys.get_state(pid).member_epoch == nil
    end

    test "an empty update is a no-op" do
      {_client, pid} = start_consumer(member_id: "m", member_epoch: 3)
      assert :ok = GenConsumer.update_membership(pid, [])
    end
  end
end
