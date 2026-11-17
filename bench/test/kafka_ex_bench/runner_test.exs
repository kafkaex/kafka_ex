defmodule KafkaExBench.RunnerTest do
  use ExUnit.Case, async: false
  import ExUnit.CaptureLog
  alias KafkaExBench.{Job, Runner}

  @keys [
    :job,
    :parallel,
    :valid,
    :error,
    :samples,
    :min_samples,
    :metrics,
    :throughput_ops,
    :p50_us,
    :p99_us,
    :max_mailbox,
    :reductions,
    :client_reductions_per_op,
    :vm_reductions_per_op,
    :vm_cpu_us_per_op,
    :background_ops
  ]

  defp opts(extra \\ []), do: Keyword.merge([warmup: 0, time: 0.2, min_samples: 1], extra)

  test "a healthy job yields a valid record" do
    job = %Job{name: "noop", parallel: 2, fun: fn -> :ok end}
    record = Runner.run_job(job, self(), opts())

    assert Enum.sort(Map.keys(record)) == Enum.sort(@keys)
    assert record.valid
    assert record.error == nil
    assert record.job == "noop"
    assert record.parallel == 2
    assert record.samples > 0
    assert record.throughput_ops > 0
    assert record.p50_us <= record.p99_us
    assert record.client_reductions_per_op >= 0
    assert record.vm_reductions_per_op > 0
    assert record.vm_cpu_us_per_op >= 0
  end

  test "the record carries the min_samples actually applied" do
    job = %Job{name: "noop", parallel: 1, fun: fn -> :ok end, min_samples: 7}

    assert Runner.run_job(job, self(), Keyword.delete(opts(), :min_samples)).min_samples == 7
    assert Runner.run_job(job, self(), opts(min_samples: 3)).min_samples == 3
  end

  test "the record carries the job's metrics, defaulting to the four standard ones" do
    job = %Job{name: "noop", parallel: 1, fun: fn -> :ok end}

    assert Runner.run_job(job, self(), opts()).metrics == [
             {:throughput_ops, :higher_better},
             {:p50_us, :lower_better},
             {:client_reductions_per_op, :lower_better},
             {:vm_reductions_per_op, :lower_better}
           ]

    custom = [{:p99_us, :lower_better}]
    assert Runner.run_job(%{job | metrics: custom}, self(), opts()).metrics == custom
  end

  test "an invalid record carries the metrics too" do
    job = %Job{name: "boom", parallel: 1, fun: fn -> raise "x" end, metrics: [{:p50_us, :lower_better}]}
    {record, _log} = with_log(fn -> Runner.run_job(job, self(), opts()) end)

    assert record.metrics == [{:p50_us, :lower_better}]
  end

  test "an invalid record carries the applied min_samples too" do
    job = %Job{name: "noop", parallel: 1, fun: fn -> :ok end}
    record = Runner.run_job(job, self(), opts(min_samples: 10_000_000))

    refute record.valid
    assert record.min_samples == 10_000_000
  end

  test "a warmup failure is labelled as such" do
    job = %Job{name: "boom", parallel: 1, fun: fn -> raise "warm-marker" end}
    {record, _log} = with_log(fn -> Runner.run_job(job, self(), opts(warmup: 0.2)) end)

    refute record.valid
    assert record.error =~ ~r/^warmup: .*warm-marker/
  end

  test "a measured-run failure is not labelled as warmup" do
    job = %Job{name: "boom", parallel: 1, fun: fn -> raise "run-marker" end}
    {record, _log} = with_log(fn -> Runner.run_job(job, self(), opts()) end)

    refute String.starts_with?(record.error, "warmup:")
  end

  test "measure: false leaves the window empty and the window metrics nil" do
    job = %Job{name: "noop", parallel: 2, fun: fn -> :ok end}
    record = Runner.run_job(job, self(), opts(measure: false))

    assert record.valid
    assert record.client_reductions_per_op == nil
    assert record.vm_reductions_per_op == nil
    assert record.vm_cpu_us_per_op == nil
  end

  test "measure: true takes the window even without a client process" do
    job = %Job{name: "noop", parallel: 2, fun: fn -> :ok end}
    record = Runner.run_job(job, nil, opts(measure: true))

    assert record.vm_reductions_per_op > 0
  end

  test "warmup runs are not part of the measured samples" do
    counter = :counters.new(1, [])
    job = %Job{name: "count", parallel: 1, fun: fn -> :counters.add(counter, 1, 1) end}
    record = Runner.run_job(job, self(), opts(warmup: 0.3, time: 0.1))

    assert record.samples < :counters.get(counter, 1)
  end

  test "client reductions burnt during warmup are excluded" do
    client = spawn_link(fn -> burn_once_loop(false) end)
    job = %Job{name: "burn", parallel: 1, fun: fn -> send(client, {:work, self()}) end}
    record = Runner.run_job(job, client, opts(warmup: 0.3, time: 0.2))

    assert record.valid
    assert record.reductions < 3_000_000
  end

  defp burn_once_loop(burnt?) do
    receive do
      {:work, _} ->
        unless burnt?, do: Enum.reduce(1..12_000_000, 0, &(&1 + &2))
        burn_once_loop(true)
    end
  end

  test "without warmup every call is a measured sample" do
    counter = :counters.new(1, [])
    job = %Job{name: "count", parallel: 1, fun: fn -> :counters.add(counter, 1, 1) end}
    record = Runner.run_job(job, self(), opts(warmup: 0, time: 0.1))

    assert record.samples == :counters.get(counter, 1)
  end

  test "an invalid record carries nil cpu metrics" do
    job = %Job{name: "boom", parallel: 1, fun: fn -> raise "x" end}
    {record, _log} = with_log(fn -> Runner.run_job(job, self(), opts()) end)

    assert %{client_reductions_per_op: nil, vm_reductions_per_op: nil, vm_cpu_us_per_op: nil} = record
  end

  test "the measured window is bracketed by the scenario hooks of every parallel worker" do
    test_pid = self()
    tracer = spawn_link(fn -> collect_marks(test_pid, []) end)
    :erlang.trace(self(), true, [:receive, {:tracer, tracer}])
    job = %Job{name: "noop", parallel: 3, fun: fn -> :ok end}
    record = Runner.run_job(job, self(), opts())
    :erlang.trace(self(), false, [:receive])
    send(tracer, :flush)

    assert record.valid
    assert_receive {:marks, marks}
    assert Enum.sort(marks) == [:after, :after, :after, :before, :before, :before]
    assert Enum.take(marks, 3) == [:before, :before, :before]
  end

  defp collect_marks(test_pid, acc) do
    receive do
      {:trace, _, :receive, {_ref, mark, {_reductions, _runtime}}} when mark in [:before, :after] ->
        collect_marks(test_pid, acc ++ [mark])

      {:trace, _, :receive, _} ->
        collect_marks(test_pid, acc)

      :flush ->
        send(test_pid, {:marks, acc})
    end
  end

  test "a raising job makes the run invalid and names the error" do
    job = %Job{name: "boom", parallel: 2, fun: fn -> raise "kaboom-marker" end}
    {record, _log} = with_log(fn -> Runner.run_job(job, self(), opts()) end)

    refute record.valid
    assert record.error =~ "kaboom-marker"
    assert record.parallel == 2
  end

  test "too few samples makes the run invalid" do
    job = %Job{name: "noop", parallel: 1, fun: fn -> :ok end}
    record = Runner.run_job(job, self(), opts(min_samples: 10_000_000))

    refute record.valid
    assert record.error =~ "too few samples"
    assert record.samples > 0
  end

  test "a job's own min_samples applies when the option is absent" do
    job = %Job{name: "noop", parallel: 1, fun: fn -> :ok end, min_samples: 10_000_000}
    record = Runner.run_job(job, self(), Keyword.delete(opts(), :min_samples))

    refute record.valid
    assert record.error =~ "too few samples"
  end

  test "an explicit min_samples option overrides the job's" do
    job = %Job{name: "noop", parallel: 1, fun: fn -> :ok end, min_samples: 10_000_000}

    assert Runner.run_job(job, self(), opts(min_samples: 1)).valid
  end

  test "min_samples defaults to 500" do
    assert %Job{name: "x", parallel: 1, fun: fn -> :ok end}.min_samples == 500
  end

  test "exceeding the overall timeout kills the run" do
    job = %Job{name: "slow", parallel: 1, fun: fn -> Process.sleep(:infinity) end}
    record = Runner.run_job(job, self(), opts(timeout_ms: 300))

    refute record.valid
    assert record.error == "timeout"
  end

  test "the sampler is stopped on every path" do
    job = %Job{name: "boom", parallel: 1, fun: fn -> raise "x" end}
    capture_log(fn -> Runner.run_job(job, self(), opts()) end)

    assert eventually(fn -> samplers() == [] end)
  end

  defp samplers do
    for pid <- Process.list(),
        {:initial_call, {KafkaExBench.Sampler, _, _}} <- [Process.info(pid, :initial_call)],
        do: pid
  end

  defp eventually(check, attempts \\ 40) do
    cond do
      check.() -> true
      attempts == 0 -> false
      true -> Process.sleep(50) && eventually(check, attempts - 1)
    end
  end

  describe "throughput/2" do
    test "is parallel over the mean latency in seconds" do
      assert Runner.throughput(4, [1_000_000, 3_000_000]) == 2000.0
    end

    test "is continuous, not tied to the sample count" do
      assert Runner.throughput(1, [333_333]) == 1.0e9 / 333_333
    end
  end

  describe "mask_home/2" do
    test "replaces the home prefix with a tilde" do
      assert Runner.mask_home("at /home/u/x and /home/u/y", "/home/u") == "at ~/x and ~/y"
    end

    test "an unset or one-character home leaves the text alone" do
      assert Runner.mask_home("/a/b", nil) == "/a/b"
      assert Runner.mask_home("/a/b", "/") == "/a/b"
      assert Runner.mask_home("/a/b", "") == "/a/b"
    end
  end

  describe "sanitize_error/1" do
    test "exception with stacktrace keeps only the banner" do
      reason = {%RuntimeError{message: "boom"}, [{Foo, :bar, 1, [file: ~c"lib/foo.ex", line: 3]}]}
      assert Runner.sanitize_error(reason) == "** (RuntimeError) boom"
    end

    test "a raw error with a stacktrace is normalised into an exception banner" do
      reason = {{:badmatch, {:error, :no_broker}}, [{Foo, :bar, 1, [file: ~c"lib/foo.ex", line: 3]}]}

      assert Runner.sanitize_error(reason) ==
               "** (MatchError) no match of right hand side value: {:error, :no_broker}"
    end

    test "a caught exit is formatted as an exit banner" do
      reason = {:caught, :exit, {:timeout, {GenServer, :call, [:x, :y, 5000]}}}

      assert Runner.sanitize_error(reason) ==
               "** (exit) exited in: GenServer.call(:x, :y, 5000) ** (EXIT) time out"
    end

    test "an exit carrying an exception from a crashed call stays free of stack frames" do
      stack = [{Foo, :bar, 1, [file: ~c"lib/foo.ex", line: 3]}]
      reason = {:caught, :exit, {{%RuntimeError{message: "x"}, stack}, {GenServer, :call, [:c, :m, 5000]}}}
      message = Runner.sanitize_error(reason)

      assert message =~ "GenServer.call(:c, :m, 5000)"
      assert message =~ "RuntimeError"
      assert message =~ "x"
      refute message =~ "lib/"
      refute message =~ "file:"
      refute message =~ "~c\""
    end

    test "an exit carrying a raw error from a crashed call stays free of stack frames" do
      stack = [{Foo, :bar, 1, [file: ~c"lib/foo.ex", line: 3]}]
      reason = {:caught, :exit, {{{:badmatch, 1}, stack}, {GenServer, :call, [:c, :m, 5000]}}}
      message = Runner.sanitize_error(reason)

      assert message =~ "MatchError"
      assert message =~ "no match of right hand side value: 1"
      refute message =~ "lib/"
      refute message =~ "file:"
      refute message =~ "~c\""
    end

    test "a raw DOWN reason of a worker that died inside GenServer.call is formatted" do
      {:ok, server} = Agent.start_link(fn -> nil end)
      Agent.cast(server, fn _ -> Process.sleep(:infinity) end)
      {pid, monitor} = spawn_monitor(fn -> Agent.get(server, & &1, 50) end)

      reason =
        receive do
          {:DOWN, ^monitor, :process, ^pid, reason} -> reason
        end

      message = Runner.sanitize_error(reason)

      assert message =~ "** (exit) exited in: GenServer.call("
      assert message =~ "time out"
      refute message =~ "{:timeout"
    end

    test "an untagged exit with a call mfa is formatted as an exit" do
      assert Runner.sanitize_error({:timeout, {GenServer, :call, [:x, :y, 5000]}}) ==
               "** (exit) exited in: GenServer.call(:x, :y, 5000) ** (EXIT) time out"
    end

    test "an untagged exit carrying an exception and a stack is stack-free" do
      stack = [{Foo, :bar, 1, [file: ~c"lib/foo.ex", line: 3]}]
      message = Runner.sanitize_error({{%RuntimeError{message: "x"}, stack}, {GenServer, :call, [:c, :m, 5000]}})

      assert message =~ "** (exit) exited in: GenServer.call(:c, :m, 5000)"
      assert message =~ "RuntimeError"
      refute message =~ "lib/"
      refute message =~ "file:"
    end

    test "a two-tuple whose second element is not a call stays inspected" do
      assert Runner.sanitize_error({:badarg, {:a, :b}}) == "{:badarg, {:a, :b}}"
    end

    test "an error tuple is data and is inspected" do
      assert Runner.sanitize_error({:error, :no_broker}) == "{:error, :no_broker}"
    end

    test "a caught throw is formatted as a throw banner" do
      assert Runner.sanitize_error({:caught, :throw, :boom}) == "** (throw) :boom"
    end

    test "an unset HOME does not raise" do
      with_home(nil, fn -> assert Runner.sanitize_error({:enoent, "/x"}) == "{:enoent, \"/x\"}" end)
    end

    test "a root HOME is not replaced" do
      with_home("/", fn -> assert Runner.sanitize_error({:enoent, "/x"}) == "{:enoent, \"/x\"}" end)
    end

    defp with_home(value, fun) do
      previous = System.get_env("HOME")
      if value, do: System.put_env("HOME", value), else: System.delete_env("HOME")

      try do
        fun.()
      after
        if previous, do: System.put_env("HOME", previous), else: System.delete_env("HOME")
      end
    end

    test "other reasons are inspected" do
      assert Runner.sanitize_error(:killed) == ":killed"
      assert Runner.sanitize_error({:badmatch, 1}) == "{:badmatch, 1}"
    end

    test "home directory is replaced with a tilde" do
      home = System.get_env("HOME")
      assert Runner.sanitize_error({:enoent, home <> "/x"}) == "{:enoent, \"~/x\"}"
    end

    test "newlines collapse to spaces" do
      assert Runner.sanitize_error(%RuntimeError{message: "a\nb\r\nc"}) == "** (RuntimeError) a b c"
    end

    test "long text is truncated to 500 characters" do
      assert Runner.sanitize_error(String.duplicate("x", 900)) |> String.length() == 500
    end
  end
end

defmodule KafkaExBench.RunnerBackgroundTest do
  use ExUnit.Case, async: false
  import ExUnit.CaptureLog
  alias KafkaExBench.{Job, Runner}

  setup do
    {:ok, log} = Agent.start_link(fn -> [] end)
    {:ok, log: log}
  end

  defp note(log, event), do: Agent.update(log, &[event | &1])
  defp events(log), do: log |> Agent.get(& &1) |> Enum.reverse()

  defp background(log, stop \\ fn _state -> {:ok, 5} end) do
    %{
      start: fn ->
        note(log, :start)
        :state
      end,
      stop: fn state ->
        note(log, {:stop, state})
        stop.(state)
      end
    }
  end

  defp job(log, extra \\ []) do
    struct!(
      %Job{
        name: "bg",
        parallel: 1,
        fun: fn -> note(log, :call) end,
        background: background(log)
      },
      extra
    )
  end

  defp opts(extra \\ []), do: Keyword.merge([warmup: 0.1, time: 0.1, min_samples: 1], extra)

  test "background starts once before the warmup run and stops once after the measured run", %{log: log} do
    record = Runner.run_job(job(log), self(), opts())
    events = events(log)

    assert record.valid
    assert hd(events) == :start
    assert List.last(events) == {:stop, :state}
    assert Enum.count(events, &(&1 == :start)) == 1
    assert Enum.count(events, &match?({:stop, _}, &1)) == 1
    assert Enum.count(events, &(&1 == :call)) > record.samples
  end

  test "a failing stop invalidates a good job with a background label", %{log: log} do
    bg = background(log, fn _ -> {:error, "broker died", 9} end)
    record = Runner.run_job(job(log, background: bg), self(), opts())

    refute record.valid
    assert record.error == "background: broker died"
    assert record.throughput_ops == nil
  end

  test "a job error is kept and the background error is appended", %{log: log} do
    bg = background(log, fn _ -> {:error, "broker died", 9} end)
    failing = job(log, background: bg, fun: fn -> raise "job-marker" end)
    {record, _} = with_log(fn -> Runner.run_job(failing, self(), opts(warmup: 0)) end)

    refute record.valid
    assert record.error =~ "job-marker"
    assert String.ends_with?(record.error, "; background: broker died")
  end

  test "the background is stopped when the job fails and its ok stop leaves the job error alone", %{log: log} do
    failing = job(log, fun: fn -> raise "job-marker" end)
    {record, _} = with_log(fn -> Runner.run_job(failing, self(), opts()) end)

    assert record.error =~ "job-marker"
    refute record.error =~ "background"
    assert {:stop, :state} in events(log)
  end

  test "a warmup failure still stops the background", %{log: log} do
    failing = job(log, fun: fn -> raise "warm-marker" end)
    {record, _} = with_log(fn -> Runner.run_job(failing, self(), opts()) end)

    assert record.error =~ "warmup: "
    assert {:stop, :state} in events(log)
  end

  test "a timeout still stops the background", %{log: log} do
    slow = job(log, fun: fn -> Process.sleep(:infinity) end)
    record = Runner.run_job(slow, self(), opts(warmup: 0, timeout_ms: 200))

    assert record.error == "timeout"
    assert {:stop, :state} in events(log)
  end

  test "a start that raises invalidates the job without running it", %{log: log} do
    bg = %{start: fn -> raise "start-marker" end, stop: fn _ -> note(log, :stopped) end}
    record = Runner.run_job(job(log, background: bg), self(), opts())

    refute record.valid
    assert record.error =~ ~r/^background: .*start-marker/
    assert events(log) == []
  end

  test "a stop that raises is a background error", %{log: log} do
    bg = background(log, fn _ -> raise "stop-marker" end)
    record = Runner.run_job(job(log, background: bg), self(), opts())

    refute record.valid
    assert record.error =~ ~r/^background: .*stop-marker/
  end

  test "the record carries the ops the background completed" do
    log = start_supervised!({Agent, fn -> [] end})
    record = Runner.run_job(job(log), self(), opts())

    assert record.background_ops == 5
  end

  test "a failing stop still reports the background ops" do
    log = start_supervised!({Agent, fn -> [] end})
    bg = background(log, fn _ -> {:error, "broker died", 9} end)
    record = Runner.run_job(job(log, background: bg), self(), opts())

    assert record.background_ops == 9
  end

  test "a job without background has nil background_ops", %{log: log} do
    record = Runner.run_job(job(log, background: nil), self(), opts())

    assert record.valid
    assert record.background_ops == nil
  end

  test "a start failure has nil background_ops", %{log: log} do
    bg = %{start: fn -> raise "start-marker" end, stop: fn _ -> {:ok, 1} end}

    assert Runner.run_job(job(log, background: bg), self(), opts()).background_ops == nil
  end
end
