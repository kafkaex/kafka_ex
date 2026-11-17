defmodule KafkaExBench.Scenarios.GatesTest do
  use ExUnit.Case, async: true
  alias KafkaExBench.Scenarios.{Basic, ColdStart, LargeFetch, Mixed, OneBroker, RestartRecovery}

  @scenarios [Basic, Mixed, OneBroker, LargeFetch, ColdStart, RestartRecovery]

  @p50 {:p50_us, :lower_better, {:relative}}
  @throughput {:throughput_ops, :higher_better, {:relative}}
  @reductions {:vm_reductions_per_op, :lower_better, {:relative}}

  test "every job has gates and every gate names a job" do
    for scenario <- @scenarios do
      names = scenario.job_specs() |> Enum.map(&elem(&1, 0)) |> Enum.sort()
      assert scenario.gates() |> Map.keys() |> Enum.sort() == names, inspect(scenario)
      assert Enum.all?(Map.values(scenario.gates()), &(&1 != [])), inspect(scenario)
    end
  end

  test "basic gates p50 everywhere, throughput above one worker and vm reductions on all" do
    gates = Basic.gates()

    assert gates["produce_c1"] == [@p50, @reductions]
    assert gates["fetch_c1"] == [@p50, @reductions]

    for name <- ["produce_c10", "produce_c100", "fetch_c10", "fetch_c100"],
        do: assert(gates[name] == [@p50, @throughput, @reductions])
  end

  test "one_broker gates p50, throughput and vm reductions" do
    for {_name, gates} <- OneBroker.gates(), do: assert(gates == [@p50, @throughput, @reductions])
  end

  test "mixed gates only p50 (single worker, background load)" do
    for {_name, gates} <- Mixed.gates(), do: assert(gates == [@p50])
  end

  test "large_fetch gates p50 and throughput" do
    for {_name, gates} <- LargeFetch.gates(), do: assert(gates == [@p50, @throughput])
  end

  test "cold_start gates p50" do
    for {_name, gates} <- ColdStart.gates(), do: assert(gates == [@p50])
  end

  test "restart_kafka2 judges both gap classes on usable runs only, lower is better" do
    assert RestartRecovery.gates() == %{
             "restart_kafka2" => [
               {:gap_other_leaders_us, :lower_better, {:usable_only}},
               {:gap_restarted_leader_us, :lower_better, {:usable_only}}
             ]
           }
  end
end
