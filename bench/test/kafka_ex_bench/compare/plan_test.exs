defmodule KafkaExBench.Compare.PlanTest do
  use ExUnit.Case, async: true
  alias KafkaExBench.Compare.Plan

  test "alternates the pair order by run parity, profile by profile, restart last" do
    plan = Plan.runs(["basic", "restart_recovery", "mixed"], [:latency, :direct], 2)

    assert Enum.map(plan, &{&1.side, &1.profile, &1.scenario, &1.index}) == [
             {:base, :latency, "basic", 1},
             {:candidate, :latency, "basic", 1},
             {:candidate, :latency, "basic", 2},
             {:base, :latency, "basic", 2},
             {:base, :latency, "mixed", 1},
             {:candidate, :latency, "mixed", 1},
             {:candidate, :latency, "mixed", 2},
             {:base, :latency, "mixed", 2},
             {:base, :direct, "basic", 1},
             {:candidate, :direct, "basic", 1},
             {:candidate, :direct, "basic", 2},
             {:base, :direct, "basic", 2},
             {:base, :direct, "mixed", 1},
             {:candidate, :direct, "mixed", 1},
             {:candidate, :direct, "mixed", 2},
             {:base, :direct, "mixed", 2},
             {:base, :latency, "restart_recovery", 1},
             {:candidate, :latency, "restart_recovery", 1},
             {:candidate, :latency, "restart_recovery", 2},
             {:base, :latency, "restart_recovery", 2},
             {:base, :direct, "restart_recovery", 1},
             {:candidate, :direct, "restart_recovery", 1},
             {:candidate, :direct, "restart_recovery", 2},
             {:base, :direct, "restart_recovery", 2}
           ]
  end

  test "odd run indexes start with base and even ones with the candidate" do
    sides = ["x"] |> Plan.runs([:latency], 4) |> Enum.map(&{&1.side, &1.index})

    assert sides == [
             {:base, 1},
             {:candidate, 1},
             {:candidate, 2},
             {:base, 2},
             {:base, 3},
             {:candidate, 3},
             {:candidate, 4},
             {:base, 4}
           ]
  end

  test "no restart scenario means no trailing block" do
    assert length(Plan.runs(["basic"], [:latency], 3)) == 6
  end

  test "output base path is unique per run and under runs/" do
    run = %{side: :candidate, profile: :latency, scenario: "basic", index: 3}
    assert Plan.output_base("out", run) == "out/runs/candidate-latency-basic-3"
  end

  test "command pins one run, the scheduler flags, the build path and the side's tree" do
    run = %{side: :candidate, profile: :latency, scenario: "basic", index: 1}
    side = %{label: "candidate", path: "/wt/cand", env: [{"BENCH_LATENCY_MS", "3"}]}
    config = %{out: "out", time: nil, allow_short: false}

    assert %{cmd: "elixir", args: args, env: env} = Plan.command(run, side, config)

    assert args == [
             "--erl",
             "+S 8:8 +sbwt none +sbwtdcpu none +sbwtdio none",
             "-S",
             "mix",
             "bench.run",
             "--scenario",
             "basic",
             "--profile",
             "latency",
             "--runs",
             "1",
             "--out",
             "out/runs/candidate-latency-basic-1"
           ]

    assert {"KAFKA_EX_PATH", "/wt/cand"} in env
    assert {"BENCH_BUILD_PATH", "_build_candidate"} in env
    assert {"MIX_ENV", "dev"} in env
    assert {"BENCH_LATENCY_MS", "3"} in env
  end

  test "compile command builds the side's tree in its own build path" do
    side = %{label: "candidate", path: "/wt/cand", env: [{"X", "1"}]}
    assert %{cmd: "mix", args: ["compile"], env: env} = Plan.compile_command(side)

    assert env == [
             {"KAFKA_EX_PATH", "/wt/cand"},
             {"BENCH_BUILD_PATH", "_build_candidate"},
             {"MIX_ENV", "dev"},
             {"X", "1"}
           ]
  end

  test "time and allow_short are passed through when set" do
    run = %{side: :base, profile: :direct, scenario: "mixed", index: 1}
    side = %{label: "base", path: "/b", env: []}
    config = %{out: "out", time: 3, allow_short: true}
    %{args: args} = Plan.command(run, side, config)
    assert ["--time", "3", "--allow-short"] == Enum.take(args, -3)
  end
end
