defmodule KafkaExBench.OneShotTest do
  use ExUnit.Case, async: true
  alias KafkaExBench.OneShot

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
    :started_at,
    :duration_ms
  ]

  defp sequence(durations) do
    {:ok, agent} = Agent.start_link(fn -> durations end)

    fn ->
      Agent.get_and_update(agent, fn [next | rest] -> {next, rest} end)
    end
  end

  test "calls fun once per repetition and reports median and max" do
    fun = sequence([{:ok, 30}, {:ok, 10}, {:ok, 20}, {:ok, 500}, {:ok, 40}])
    record = OneShot.measure("cold", 5, fun, [])

    assert Enum.sort(Map.keys(record)) == Enum.sort(@keys)
    assert record.valid
    assert record.error == nil
    assert record.job == "cold"
    assert record.parallel == 1
    assert record.samples == 5
    assert record.min_samples == 5
    assert record.p50_us == 30
    assert record.p99_us == 500
    assert record.metrics == [{:p50_us, :lower_better}]
  end

  test "a OneShotJob discards no repetition by default" do
    assert %KafkaExBench.OneShotJob{name: "x", repetitions: 1, fun: fn -> :ok end}.discard == 0
  end

  test "the throughput and cpu fields are nil" do
    record = OneShot.measure("cold", 1, fn -> {:ok, 7} end, [])

    assert %{
             throughput_ops: nil,
             max_mailbox: nil,
             reductions: nil,
             client_reductions_per_op: nil,
             vm_reductions_per_op: nil,
             vm_cpu_us_per_op: nil
           } = record
  end

  test "the record is timestamped" do
    record = OneShot.measure("cold", 1, fn -> {:ok, 7} end, [])

    assert {:ok, _, 0} = DateTime.from_iso8601(record.started_at)
    assert is_integer(record.duration_ms) and record.duration_ms >= 0
  end

  test "repetitions run sequentially" do
    parent = self()

    fun = fn ->
      send(parent, {:enter, self()})
      Process.sleep(5)
      send(parent, {:leave, self()})
      {:ok, 1}
    end

    OneShot.measure("seq", 3, fun, [])

    messages = for _ <- 1..6, do: receive(do: ({tag, _} -> tag))
    assert messages == [:enter, :leave, :enter, :leave, :enter, :leave]
  end

  test "an error result invalidates the record and stops further repetitions" do
    fun = sequence([{:ok, 1}, {:error, {:badmatch, :nope}}, {:ok, 3}])
    record = OneShot.measure("cold", 3, fun, [])

    refute record.valid
    assert record.error == "{:badmatch, :nope}"
    assert record.p50_us == nil
    assert record.p99_us == nil
    assert record.samples == 0
    assert record.metrics == [{:p50_us, :lower_better}]
  end

  test "a raising fun invalidates the record with a sanitised error" do
    record = OneShot.measure("cold", 2, fn -> raise "shot-marker" end, [])

    refute record.valid
    assert record.error =~ "shot-marker"
    refute record.error =~ "lib/"
  end

  test "an unexpected result invalidates the record" do
    record = OneShot.measure("cold", 1, fn -> :weird end, [])

    refute record.valid
    assert record.error =~ ":weird"
  end

  test "an exit inside fun invalidates the record" do
    record = OneShot.measure("cold", 1, fn -> exit(:boom) end, [])

    refute record.valid
    assert record.error =~ "boom"
  end

  test "a repetition that exceeds timeout_ms is killed and invalidates the record" do
    parent = self()

    fun = fn ->
      send(parent, {:running, self()})
      Process.sleep(:infinity)
    end

    record = OneShot.measure("hang", 3, fun, timeout_ms: 50)

    refute record.valid
    assert record.error == "timeout after 50 ms"
    assert_receive {:running, pid}
    refute_receive {:running, _}, 20
    refute Process.alive?(pid)
  end

  test "the timeout applies per repetition, not to the whole job" do
    record = OneShot.measure("slow", 3, fn -> Process.sleep(40) && {:ok, 1} end, timeout_ms: 100)

    assert record.valid
    assert record.samples == 3
  end

  test "processes linked to a timed-out repetition die with it" do
    parent = self()

    fun = fn ->
      child = spawn_link(fn -> Process.sleep(:infinity) end)
      send(parent, {:child, child})
      Process.sleep(:infinity)
    end

    OneShot.measure("hang", 1, fun, timeout_ms: 50)

    assert_receive {:child, child}
    Process.sleep(20)
    refute Process.alive?(child)
  end

  test "extras returned by a repetition are summed into the record" do
    {:ok, agent} = Agent.start_link(fn -> [{:ok, 1, %{errors_during: 2}}, {:ok, 3, %{errors_during: 5}}] end)
    fun = fn -> Agent.get_and_update(agent, fn [next | rest] -> {next, rest} end) end

    record = OneShot.measure("extra", 2, fun, [])

    assert record.valid
    assert record.errors_during == 7
    assert record.p50_us == 2
  end

  test "discarded repetitions run first and leave no trace in samples or extras" do
    {:ok, agent} =
      Agent.start_link(fn ->
        [{:ok, 9_000, %{errors_during: 100}}, {:ok, 10, %{errors_during: 1}}, {:ok, 30, %{errors_during: 2}}]
      end)

    fun = fn -> Agent.get_and_update(agent, fn [next | rest] -> {next, rest} end) end

    record = OneShot.measure("warm", 2, fun, discard: 1)

    assert record.valid
    assert record.samples == 2
    assert record.p50_us == 20
    assert record.p99_us == 30
    assert record.errors_during == 3
    assert Agent.get(agent, & &1) == []
  end

  test "a failing discarded repetition invalidates the record" do
    fun = sequence([{:error, :cold}, {:ok, 1}])
    record = OneShot.measure("warm", 1, fun, discard: 1)

    refute record.valid
    assert record.error == ":cold"
  end

  test "list extras are concatenated, not summed" do
    {:ok, agent} = Agent.start_link(fn -> [{:ok, 1, %{layouts: [%{0 => 2}]}}, {:ok, 3, %{layouts: [%{0 => 1}]}}] end)
    fun = fn -> Agent.get_and_update(agent, fn [next | rest] -> {next, rest} end) end

    record = OneShot.measure("extra", 2, fun, [])

    assert record.layouts == [%{0 => 2}, %{0 => 1}]
  end
end
