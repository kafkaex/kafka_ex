defmodule KafkaExBench.ReportTest do
  use ExUnit.Case, async: true
  alias KafkaExBench.Report

  setup do
    dir = Path.join(System.tmp_dir!(), "bench_report_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    {:ok, base: Path.join(dir, "out")}
  end

  defp record(job, tp, p99, valid \\ true) do
    %{
      job: job,
      parallel: 4,
      valid: valid,
      error: if(valid, do: nil, else: "timeout"),
      samples: 10,
      min_samples: 500,
      metrics: [{:throughput_ops, :higher_better}, {:p50_us, :lower_better}],
      throughput_ops: tp,
      p50_us: 100,
      p99_us: p99,
      max_mailbox: 3,
      reductions: 1000,
      client_reductions_per_op: if(valid, do: 50.0),
      vm_reductions_per_op: if(valid, do: tp / 10),
      vm_cpu_us_per_op: if(valid, do: tp / 100)
    }
  end

  defp report do
    %{
      env: %{profile: :direct, otp: "27", kafka_ex_sha: "abc", harness_sha: nil},
      config: %{scenario: "produce", profile: :direct, runs: 3, warmup: 2, time: 10, min_samples: 500, latency_ms: nil},
      scenario_params: %{partitions: 6, acks: -1},
      scenario: "produce",
      started_at: "2026-11-16T10:00:00Z",
      runs: [
        %{run: 1, layout: %{0 => "n1", 1 => "n2"}, jobs: [record("produce_1", 100.0, 500)]},
        %{run: 2, layout: %{0 => "n1", 1 => "n2"}, jobs: [record("produce_1", 120.0, 700)]},
        %{run: 3, layout: %{0 => "n1", 1 => "n2"}, jobs: [record("produce_1", nil, nil, false)]}
      ]
    }
  end

  test "writes json that decodes back", %{base: base} do
    Report.write!(base, report())
    decoded = base |> Kernel.<>(".json") |> File.read!() |> Jason.decode!()

    assert decoded["scenario"] == "produce"
    assert length(decoded["runs"]) == 3
    assert decoded["env"]["profile"] == "direct"
  end

  test "metrics are written to json as a key to direction map" do
    base = Path.join(System.tmp_dir!(), "bench_metrics_#{System.unique_integer([:positive])}/out")
    on_exit(fn -> File.rm_rf!(Path.dirname(base)) end)
    Report.write!(base, report())
    decoded = base |> Kernel.<>(".json") |> File.read!() |> Jason.decode!()

    assert [%{"metrics" => %{"throughput_ops" => "higher_better", "p50_us" => "lower_better"}} | _] =
             hd(decoded["runs"])["jobs"]
  end

  test "markdown has job table, median and invalid count", %{base: base} do
    Report.write!(base, report())
    md = File.read!(base <> ".md")

    assert md =~ "produce_1"

    assert md =~
             "| run | valid | min samples | throughput ops/s | p50 µs | p99 µs | max mailbox | reductions | client reductions/op | vm reductions/op | vm cpu µs/op | error |"

    assert md =~ "median throughput: 110.0 ops/s"
    assert md =~ "median p99: 600.0 µs"
    assert md =~ "median vm cpu: 1.1 µs/op"
    assert md =~ "| 1 | true | 500 | 100.0 | 100 | 500 | 3 | 1000 | 50.0 | 10.0 | 1.0 |  |"
    assert md =~ "throughput CV: 12.9%"
    assert md =~ "invalid runs: 1"
  end

  test "markdown has a configuration block after the environment", %{base: base} do
    Report.write!(base, report())
    md = File.read!(base <> ".md")

    assert md =~ "## Configuration"
    assert md =~ "- warmup: 2"
    assert md =~ "- latency_ms: n/a"
    assert md =~ "- partitions: 6"
    assert :binary.match(md, "## Environment") < :binary.match(md, "## Configuration")
    assert :binary.match(md, "## Configuration") < :binary.match(md, "## produce_1")
  end

  test "a missing sha is rendered as MISSING", %{base: base} do
    Report.write!(base, report())
    md = File.read!(base <> ".md")

    assert md =~ "- harness_sha: MISSING"
    assert md =~ "- kafka_ex_sha: abc"
  end

  test "run timestamps are part of the json", %{base: base} do
    run = %{run: 1, layout: %{}, started_at: "2026-11-16T10:00:00Z", duration_ms: 5, jobs: []}
    Report.write!(base, %{report() | runs: [run]})
    decoded = base |> Kernel.<>(".json") |> File.read!() |> Jason.decode!()

    assert [%{"started_at" => "2026-11-16T10:00:00Z", "duration_ms" => 5}] = decoded["runs"]
  end

  test "markdown shows the background ops of jobs that have a background", %{base: base} do
    with_ops =
      for {rec, ops} <- [{record("produce_1", 100.0, 500), 1000}, {record("produce_1", 120.0, 700), 3000}],
          do: Map.put(rec, :background_ops, ops)

    runs = for {rec, index} <- Enum.with_index(with_ops, 1), do: %{run: index, layout: %{}, jobs: [rec]}
    Report.write!(base, %{report() | runs: runs})
    md = File.read!(base <> ".md")

    assert md =~ "- median background ops: 2000"
  end

  test "markdown has no background line without a background", %{base: base} do
    Report.write!(base, report())

    refute File.read!(base <> ".md") =~ "background ops"
  end

  describe "list params" do
    test "are rendered with inspect", %{base: base} do
      Report.write!(base, %{report() | scenario_params: %{partition_counts: [1, 10, 100]}})

      assert File.read!(base <> ".md") =~ "- partition_counts: [1, 10, 100]"
    end
  end

  describe "security line" do
    defp md_for(base, config_security, params) do
      config = Map.put(report().config, :security, config_security)
      Report.write!(base, %{report() | config: config, scenario_params: params})
      File.read!(base <> ".md")
    end

    test "scenario params win and the differing cli value is shown as cli_security", %{base: base} do
      md = md_for(base, :ssl, %{security: :scram})

      assert length(Regex.scan(~r/^- security:/m, md)) == 1
      assert md =~ "- security: scram"
      assert md =~ "- cli_security: ssl"
    end

    test "equal values are printed once without cli_security", %{base: base} do
      md = md_for(base, :scram, %{security: :scram})

      assert length(Regex.scan(~r/^- security:/m, md)) == 1
      refute md =~ "cli_security"
    end

    test "without a scenario security the cli value is the security", %{base: base} do
      md = md_for(base, :ssl, %{partitions: 6})

      assert md =~ "- security: ssl"
      refute md =~ "cli_security"
    end
  end

  describe "one-shot extras" do
    defp shot(extra), do: Map.merge(record("restart_kafka2", 1.0, 1), Map.merge(%{throughput_ops: nil}, extra))

    defp md_with(base, shots) do
      runs = for {rec, index} <- Enum.with_index(shots, 1), do: %{run: index, layout: %{}, jobs: [rec]}
      Report.write!(base, %{report() | runs: runs})
      File.read!(base <> ".md")
    end

    test "gaps, rejoin, errors, retries and layouts are summarised", %{base: base} do
      layout = %{0 => 2, 1 => 1, 2 => 3}

      md =
        md_with(base, [
          shot(%{
            gap_restarted_leader_us: [1_500_000, 1_700_000],
            gap_other_leaders_us: [20_000, 40_000],
            rejoin_ms: [3_000, 5_000],
            errors_during_repetitions: 2,
            fetch_retries: 4,
            layouts: [layout, layout]
          }),
          shot(%{
            gap_restarted_leader_us: [1_900_000],
            gap_other_leaders_us: [60_000],
            rejoin_ms: [7_000],
            errors_during_repetitions: 0,
            fetch_retries: 1,
            layouts: [layout]
          })
        ])

      assert md =~ "- median gap restarted leader: 1700000 µs"
      assert md =~ "- median gap other leaders: 40000 µs"
      assert md =~ "- median rejoin: 5000 ms"
      assert md =~ "- errors during repetitions: 2"
      assert md =~ "- fetch retries: 5"
      assert md =~ "- layouts: 3 recorded, 1 distinct"
    end

    test "restart timestamps are summarised as medians", %{base: base} do
      md =
        md_with(base, [
          shot(%{
            restart_cmd_started_ms: [5_000, 5_100],
            restart_cmd_returned_ms: [9_000, 9_200],
            gap_restarted_leader_start_ms: [5_300],
            gap_restarted_leader_end_ms: [7_300],
            gap_other_leaders_start_ms: [5_400],
            gap_other_leaders_end_ms: [5_500]
          })
        ])

      assert md =~ "- median restart command started: 5050 ms"
      assert md =~ "- median restart command returned: 9100 ms"
      assert md =~ "- median gap restarted leader start: 5300 ms"
      assert md =~ "- median gap restarted leader end: 7300 ms"
      assert md =~ "- median gap other leaders start: 5400 ms"
      assert md =~ "- median gap other leaders end: 5500 ms"
    end

    test "list extras survive into the json", %{base: base} do
      extras = %{gap_restarted_leader_us: [1, 2], gap_other_leaders_us: [3], rejoin_ms: [4], layouts: [%{0 => 2}]}
      md_with(base, [shot(extras)])
      [%{"jobs" => [job]}] = (base <> ".json") |> File.read!() |> Jason.decode!() |> Map.fetch!("runs")

      assert job["gap_restarted_leader_us"] == [1, 2]
      assert job["gap_other_leaders_us"] == [3]
      assert job["rejoin_ms"] == [4]
      assert job["layouts"] == [%{"0" => 2}]
    end

    test "a job without extras prints none of these lines", %{base: base} do
      md = md_with(base, [record("produce_1", 100.0, 500)])

      for label <- ["gap restarted", "rejoin", "errors during", "fetch retries", "layouts"] do
        refute md =~ label
      end
    end
  end

  test "cv is n/a with a single valid run", %{base: base} do
    r = %{report() | runs: [hd(report().runs)]}
    Report.write!(base, r)
    assert File.read!(base <> ".md") =~ "throughput CV: n/a"
  end

  test "a pipe in an error does not break the table row", %{base: base} do
    bad = %{record("produce_1", nil, nil, false) | error: "a | b"}
    Report.write!(base, %{report() | runs: [%{run: 1, layout: %{}, jobs: [bad]}]})
    assert File.read!(base <> ".md") =~ "| a \\| b |"
  end
end
