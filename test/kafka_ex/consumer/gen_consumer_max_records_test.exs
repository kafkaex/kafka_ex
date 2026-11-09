defmodule KafkaEx.Consumer.GenConsumerMaxRecordsTest do
  @moduledoc """
  `:max_records`: one `handle_message_set/2` call receives at most that many
  records; the rest of the fetch is handed out in later passes of the
  consumer's loop, before the next fetch, and every commit covers only the
  chunks already handed to the callback.

  Driven through the real consume loop against a client that serves a fixed
  log (a fetch at offset `o` returns every record from `o` on), wrapping
  `KafkaEx.Test.MockClient` so every request is recorded.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog
  import KafkaEx.TestSupport.ProcessHelpers

  alias KafkaEx.Consumer.ConsumerGroup
  alias KafkaEx.Consumer.GenConsumer
  alias KafkaEx.Consumer.GenConsumer.Supervisor, as: GenConsumerSupervisor
  alias KafkaEx.Messages.Fetch
  alias KafkaEx.Messages.Fetch.Record
  alias KafkaEx.Test.MockClient

  # Reports every chunk; replies with the commit mode it was started with.
  defmodule ReportingConsumer do
    use KafkaEx.Consumer.GenConsumer

    def init(_topic, _partition, %{test_pid: _} = args) do
      {:ok, Map.merge(%{mode: :sync_commit, block: false}, args)}
    end

    def handle_message_set(records, state) do
      send(state.test_pid, {:chunk, self(), Enum.map(records, & &1.offset)})

      # Blocks until the test lets the chunk finish, so it can queue a call
      # or a stop while the consumer is inside a chunk.
      if state.block do
        receive do
          :go -> :ok
        end
      end

      {state.mode, state}
    end

    def handle_call(:ping, _from, state), do: {:reply, :pong, state}
  end

  # A broker log of `count` records (offsets 0..count-1). A fetch returns
  # every record at or after the requested offset; a scripted {:error, _}
  # fetch response on the inner MockClient is returned instead.
  defmodule LogClient do
    use GenServer

    def start_link({count, responses}), do: GenServer.start_link(__MODULE__, {count, responses})

    def init({count, responses}) do
      {:ok, mock} = MockClient.start_link(responses)
      {:ok, %{mock: mock, count: count}}
    end

    def handle_call({:fetch, _topic, _partition, offset, _opts} = msg, _from, %{mock: mock, count: count} = state) do
      case GenServer.call(mock, msg) do
        {:error, _} = error ->
          {:reply, error, state}

        {:ok, _} ->
          records = for o <- offset..(count - 1)//1, do: %Record{offset: o, value: "v#{o}"}
          {:reply, {:ok, %Fetch{topic: "t", partition: 0, records: records}}, state}
      end
    end

    def handle_call(:inner, _from, state), do: {:reply, state.mock, state}
    def handle_call(msg, _from, %{mock: mock} = state), do: {:reply, GenServer.call(mock, msg), state}
  end

  @base_responses %{
    offset_fetch: {:ok, [%{partition_offsets: [%{offset: 0, error_code: :no_error}]}]},
    offset_commit: {:ok, []}
  }

  defp start_client(count, responses \\ %{}) do
    {:ok, client} = LogClient.start_link({count, Map.merge(@base_responses, responses)})
    Process.unlink(client)
    on_exit(fn -> stop_safely(client) end)
    client
  end

  defp start_consumer(client, opts, args \\ %{}) do
    args = Map.merge(%{test_pid: self()}, args)
    opts = [client: client, extra_consumer_args: args] ++ opts
    {:ok, pid} = GenServer.start_link(GenConsumer, {ReportingConsumer, "g", "t", 0, opts})
    Process.unlink(pid)
    on_exit(fn -> stop_safely(pid) end)
    pid
  end

  defp inner(client), do: GenServer.call(client, :inner)

  defp fetch_offsets(client), do: for({:fetch, "t", 0, offset, _} <- MockClient.get_calls(client), do: offset)

  defp commits(client) do
    for {:offset_commit, "g", [{"t", [%{partition_num: 0, offset: offset}]}], opts} <- MockClient.get_calls(client),
        do: {offset, opts[:member_epoch]}
  end

  defp commit_offsets(client), do: Enum.map(commits(client), &elem(&1, 0))

  defp collect_chunks(pid, count) do
    for _ <- 1..count do
      assert_receive {:chunk, ^pid, offsets}, 2_000
      offsets
    end
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

  defp wait_for_queued_message(pid),
    do: wait_until(fn -> match?({:messages, [_ | _]}, Process.info(pid, :messages)) end)

  describe "without :max_records" do
    test "the whole fetch goes to one call, as before" do
      client = start_client(10)
      pid = start_consumer(client, [])

      assert collect_chunks(pid, 1) == [Enum.to_list(0..9)]
      refute_receive {:chunk, ^pid, _}, 100
      wait_until(fn -> 10 in fetch_offsets(client) end)
      assert [0, 10 | _] = fetch_offsets(client)
      assert [10 | _] = commit_offsets(client)
      assert :sys.get_state(pid).pending_records == []
    end
  end

  describe "chunking" do
    test "N records with max_records k: ceil(N/k) calls in order, one fetch, each chunk committed (sync)" do
      client = start_client(10)
      pid = start_consumer(client, max_records: 3)

      assert collect_chunks(pid, 4) == [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9]]
      refute_receive {:chunk, ^pid, _}, 100

      # The set was fetched once; the next fetch starts after its last record.
      wait_until(fn -> 10 in fetch_offsets(client) end)
      assert [0, 10 | rest] = fetch_offsets(client)
      assert Enum.all?(rest, &(&1 == 10))

      # :sync_commit commits each chunk's end, before the next chunk.
      assert commit_offsets(client) == [3, 6, 9, 10]
    end

    test "max_records equal to the fetch size is a single call" do
      client = start_client(4)
      pid = start_consumer(client, max_records: 4)

      assert collect_chunks(pid, 1) == [[0, 1, 2, 3]]
      refute_receive {:chunk, ^pid, _}, 100
    end

    test ":async_commit acks only the chunk: the threshold sees each chunk separately" do
      client = start_client(10)
      _pid = start_consumer(client, [max_records: 3, commit_threshold: 3], %{mode: :async_commit})

      # Chunks of 3 reach a threshold of 3 one by one; the last chunk (1
      # record) does not.
      wait_until(fn -> length(commit_offsets(client)) >= 3 end)
      Process.sleep(50)
      assert commit_offsets(client) == [3, 6, 9]
    end

    test "the process span's message_count is the chunk size" do
      handler_id = "max-records-process-#{System.unique_integer([:positive])}"
      test_pid = self()

      :telemetry.attach(
        handler_id,
        [:kafka_ex, :consumer, :process, :start],
        fn _name, _measurements, metadata, _ -> send(test_pid, {:process_start, metadata.message_count}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler_id) end)

      client = start_client(7)
      pid = start_consumer(client, max_records: 3)
      collect_chunks(pid, 3)

      for expected <- [3, 3, 1], do: assert_receive({:process_start, ^expected})
      refute_receive {:process_start, _}, 50
    end
  end

  describe "interleaving between chunks" do
    test "a call and update_membership/3 are handled between chunks; the next chunk commits with the new epoch" do
      client = start_client(9)
      pid = start_consumer(client, [max_records: 3, member_id: "m", member_epoch: 3], %{block: true})

      assert_receive {:chunk, ^pid, [0, 1, 2]}, 2_000
      update = Task.async(fn -> GenConsumer.update_membership(pid, member_epoch: 4) end)
      call = Task.async(fn -> GenConsumer.call(pid, :ping) end)
      wait_until(fn -> match?({:messages, [_, _ | _]}, Process.info(pid, :messages)) end)
      send(pid, :go)

      # Both are answered before the second chunk is finished, i.e. between
      # the chunks, not after the whole fetched set.
      assert_receive {:chunk, ^pid, [3, 4, 5]}, 2_000
      assert Task.await(update) == :ok
      assert Task.await(call) == :pong

      # Snapshot right after the second chunk: the next fetch position is past
      # the buffered records, the ack (what any commit sends) only past the
      # chunks handed out.
      snapshot = Task.async(fn -> :sys.get_state(pid) end)
      wait_for_queued_message(pid)
      send(pid, :go)

      assert %{current_offset: 9, acked_offset: 6, pending_records: [%Record{offset: 6}, _, %Record{offset: 8}]} =
               Task.await(snapshot)

      assert_receive {:chunk, ^pid, [6, 7, 8]}, 2_000
      send(pid, :go)

      wait_until(fn -> length(commits(client)) >= 3 end)
      assert Enum.take(commits(client), 3) == [{3, 3}, {6, 4}, {9, 4}]
      assert fetch_offsets(client) |> Enum.take(2) == [0, 9]
    end
  end

  describe "stopping with records buffered" do
    setup do
      client = start_client(10)

      {:ok, sup} =
        GenConsumerSupervisor.start_link(%{
          gen_consumer_module: GenConsumer,
          consumer_module: ReportingConsumer,
          group_name: "g",
          assignments: [],
          opts: [
            client: client,
            max_records: 3,
            commit_threshold: 1_000_000,
            commit_interval: 600_000,
            extra_consumer_args: %{test_pid: self(), mode: :async_commit, block: true}
          ]
        })

      Process.unlink(sup)
      on_exit(fn -> stop_safely(sup) end)

      {:ok, pid} = GenConsumerSupervisor.start_partition(sup, {"t", 0}, member_id: "m", member_epoch: 3)

      # Two chunks processed (acked up to 6); stop requested inside the second.
      assert_receive {:chunk, ^pid, [0, 1, 2]}, 2_000
      send(pid, :go)
      assert_receive {:chunk, ^pid, [3, 4, 5]}, 2_000

      %{client: client, sup: sup, pid: pid}
    end

    test "stop_partition/3: the terminal commit covers only processed chunks", ctx do
      stop = Task.async(fn -> GenConsumerSupervisor.stop_partition(ctx.sup, {"t", 0}) end)
      wait_for_queued_message(ctx.pid)
      send(ctx.pid, :go)

      assert Task.await(stop) == :ok
      refute_received {:chunk, _, _}
      assert commits(ctx.client) == [{6, 3}]
    end

    test "stop_partition/3 lost: true commits nothing", ctx do
      stop = Task.async(fn -> GenConsumerSupervisor.stop_partition(ctx.sup, {"t", 0}, lost: true) end)
      wait_for_queued_message(ctx.pid)
      send(ctx.pid, :go)

      assert Task.await(stop) == :ok
      refute_received {:chunk, _, _}
      assert commits(ctx.client) == []
    end
  end

  describe "backoffs" do
    test "a :stale_member_epoch commit backoff does not hold buffered chunks back; they are committed later" do
      client = start_client(10, %{offset_commit: {:error, :stale_member_epoch}})

      capture_log(fn ->
        pid = start_consumer(client, max_records: 3, member_id: "m", member_epoch: 3, commit_interval: 60_000)

        # The first chunk's commit is rejected; the other chunks are still
        # handed out without another commit attempt.
        assert collect_chunks(pid, 4) == [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9]]
        wait_until(fn -> 10 in fetch_offsets(client) end)
        assert commits(client) == [{3, 3}]
        assert %{acked_offset: 10, current_offset: 10, pending_records: []} = :sys.get_state(pid)

        :ok = MockClient.put_response(inner(client), :offset_commit, {:ok, []})
        :ok = GenConsumer.update_membership(pid, member_epoch: 4)
        stop_safely(pid)
      end)

      # The terminal commit carries every acked chunk, with the new epoch.
      assert commits(client) == [{3, 3}, {10, 4}]
    end

    test "a fetch backoff after the buffer drains resumes after the last buffered record" do
      Application.put_env(:kafka_ex, :fetch_retry_base_delay_ms, 10)
      Application.put_env(:kafka_ex, :fetch_retry_max_delay_ms, 20)

      on_exit(fn ->
        Application.delete_env(:kafka_ex, :fetch_retry_base_delay_ms)
        Application.delete_env(:kafka_ex, :fetch_retry_max_delay_ms)
      end)

      client = start_client(5)
      pid = start_consumer(client, [max_records: 2], %{block: true})

      assert_receive {:chunk, ^pid, [0, 1]}, 2_000
      # Every fetch after the first one fails until the test clears it.
      :ok = MockClient.put_response(inner(client), :fetch, {:error, :no_broker})
      send(pid, :go)
      assert_receive {:chunk, ^pid, [2, 3]}, 2_000
      send(pid, :go)
      assert_receive {:chunk, ^pid, [4]}, 2_000
      send(pid, :go)

      capture_log(fn -> wait_until(fn -> length(fetch_offsets(client)) >= 3 end) end)
      assert [0 | retries] = fetch_offsets(client)
      assert Enum.all?(retries, &(&1 == 5))
      assert commit_offsets(client) |> Enum.take(3) == [2, 4, 5]
    end
  end

  describe "validation" do
    test "an invalid :max_records fails start_link with {:error, :invalid_max_records}" do
      Process.flag(:trap_exit, true)
      client = start_client(1)

      for bad <- [0, -1, 1.5, "3", :all] do
        assert GenConsumer.start_link(ReportingConsumer, "g", "t", 0,
                 client: client,
                 max_records: bad,
                 extra_consumer_args: %{test_pid: self()}
               ) == {:error, :invalid_max_records},
               inspect(bad)
      end

      assert MockClient.get_calls(inner(client)) == []
    end

    test "nil and positive integers are accepted" do
      assert GenConsumer.valid_max_records?(nil)
      assert GenConsumer.valid_max_records?(1)
      assert GenConsumer.valid_max_records?(500)
    end

    test "ConsumerGroup.start_link/4 refuses an invalid :max_records before anything starts" do
      for protocol <- [:classic, :consumer], bad <- [0, -5, "10"] do
        log =
          capture_log(fn ->
            assert ConsumerGroup.start_link(ReportingConsumer, "g", ["t"], group_protocol: protocol, max_records: bad) ==
                     {:error, {:invalid_option, :invalid_max_records}}
          end)

        assert log =~ "Invalid :max_records #{inspect(bad)}"
      end
    end

    test "ConsumerGroup passes :max_records on to both managers, without an ignored-option warning" do
      log =
        capture_log(fn ->
          for protocol <- [:classic, :consumer] do
            {:ok, {_flags, [child]}} =
              ConsumerGroup.init(
                {{GenConsumer, ReportingConsumer}, "g", ["t"], [group_protocol: protocol, max_records: 7]}
              )

            {_module, :start_link, [{_, _, _, opts}]} = child.start
            assert opts[:max_records] == 7
          end
        end)

      refute log =~ "ignores the options"
    end
  end
end
