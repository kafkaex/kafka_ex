defmodule Mix.Tasks.Bench.RunTest do
  use ExUnit.Case, async: true
  alias Mix.Tasks.Bench.Run

  test "defaults and required options" do
    assert Run.parse!(~w(--scenario basic --profile direct --runs 5 --out out/r)) == %{
             scenario: KafkaExBench.Scenarios.Basic,
             scenario_name: "basic",
             profile: :direct,
             security: :ssl,
             runs: 5,
             warmup: 2,
             time: 10,
             short: false,
             out: "out/r"
           }
  end

  test "explicit values" do
    opts = Run.parse!(~w(--scenario basic --profile latency --security scram --runs 3 --warmup 1 --time 12 --out x))
    assert %{profile: :latency, security: :scram, runs: 3, warmup: 1, time: 12, short: false} = opts
  end

  test "time and warmup default to the scenario's own" do
    assert %{time: 30, warmup: 2} = Run.parse!(~w(--scenario mixed --out x))
    assert %{time: 15, warmup: 2} = Run.parse!(~w(--scenario large_fetch --out x))
    assert %{time: 0, warmup: 0} = Run.parse!(~w(--scenario cold_start --out x))
  end

  test "a time below the scenario default is refused with the default named" do
    assert_raise Mix.Error, ~r/--time 5 is below the mixed default of 30 s/, fn ->
      Run.parse!(~w(--scenario mixed --time 5 --out x))
    end
  end

  test "a short time is allowed with --allow-short and recorded" do
    assert %{time: 5, short: true} = Run.parse!(~w(--scenario mixed --time 5 --allow-short --out x))
    assert %{time: 30, short: false} = Run.parse!(~w(--scenario mixed --allow-short --out x))
  end

  test "--out is required" do
    assert_raise Mix.Error, ~r/--out/, fn -> Run.parse!(~w(--scenario basic --profile direct --runs 5)) end
  end

  test "the large_fetch scenario is selectable by name" do
    assert Run.parse!(~w(--scenario large_fetch --out x)).scenario == KafkaExBench.Scenarios.LargeFetch
  end

  test "the one_broker scenario is selectable by name" do
    assert Run.parse!(~w(--scenario one_broker --out x)).scenario == KafkaExBench.Scenarios.OneBroker
  end

  test "the mixed scenario is selectable by name" do
    assert Run.parse!(~w(--scenario mixed --out x)).scenario == KafkaExBench.Scenarios.Mixed
  end

  test "the cold_start scenario is selectable by name" do
    assert Run.parse!(~w(--scenario cold_start --out x)).scenario == KafkaExBench.Scenarios.ColdStart
  end

  test "the restart_recovery scenario is selectable by name" do
    assert Run.parse!(~w(--scenario restart_recovery --out x)).scenario == KafkaExBench.Scenarios.RestartRecovery
  end

  test "restart_recovery is accepted under the latency profile" do
    assert Run.parse!(~w(--scenario restart_recovery --profile latency --out x)).profile == :latency
  end

  test "restart_recovery is accepted under the direct profile" do
    assert Run.parse!(~w(--scenario restart_recovery --profile direct --out x)).profile == :direct
  end

  test "other scenarios still run under the latency profile" do
    assert Run.parse!(~w(--scenario cold_start --profile latency --out x)).profile == :latency
  end

  test "unknown scenario is rejected" do
    assert_raise Mix.Error, ~r/unknown scenario nope/, fn ->
      Run.parse!(~w(--scenario nope --profile direct --runs 5 --out x))
    end
  end

  test "a job line shows the run, job and outcome" do
    job = %{job: "produce_c1", valid: true, throughput_ops: 1234.56, p50_us: 10, p99_us: 20, max_mailbox: 3, error: nil}

    assert Run.job_line(2, job) == "run=2 job=produce_c1 valid=true thr=1234.6 p50=10 p99=20 mbox=3"
  end

  test "a job line shows the error of an invalid job" do
    job = %{job: "x", valid: false, throughput_ops: nil, p50_us: nil, p99_us: nil, max_mailbox: nil, error: "timeout"}

    assert Run.job_line(1, job) =~ "valid=false thr=n/a p50=n/a p99=n/a mbox=n/a error=timeout"
  end

  test "leaked topics produce a warning naming the count" do
    runs = [%{run: 1, leaked_bench_topics: 0}, %{run: 2, leaked_bench_topics: 3}, %{run: 3, leaked_bench_topics: nil}]

    assert Run.leak_warning(runs) =~ "run 2: 3"
    refute Run.leak_warning(runs) =~ "run 1"
  end

  test "no leaked topics means no warning" do
    assert Run.leak_warning([%{run: 1, leaked_bench_topics: 0}, %{run: 2, leaked_bench_topics: nil}]) == nil
  end

  test "missing shas produce warnings" do
    assert Run.env_warnings(%{kafka_ex_sha: nil, harness_sha: nil}) |> length() == 2
    assert Run.env_warnings(%{kafka_ex_sha: "a", harness_sha: nil}) == ["harness_sha is MISSING"]
    assert Run.env_warnings(%{kafka_ex_sha: "a", harness_sha: "b"}) == []
  end

  test "the report config lists the run settings" do
    config = Run.parse!(~w(--scenario basic --profile latency --security scram --runs 3 --warmup 1 --time 12 --out x))
    env = %{latency_ms: 7}

    assert Run.report_config(config, env) == %{
             scenario: "basic",
             profile: :latency,
             security: :scram,
             runs: 3,
             warmup: 1,
             time: 12,
             short: false,
             latency_ms: 7
           }
  end

  test "an invalidation line names the run and the reason" do
    assert Run.invalidation_line(3, "teardown failed: x") == "run=3 invalidated: teardown failed: x"
  end

  test "runs must be positive" do
    assert_raise Mix.Error, ~r/--runs/, fn -> Run.parse!(~w(--scenario basic --profile direct --runs 0 --out x)) end
  end

  test "unknown flags are rejected" do
    assert_raise OptionParser.ParseError, fn -> Run.parse!(~w(--bogus 1 --out x)) end
  end
end
