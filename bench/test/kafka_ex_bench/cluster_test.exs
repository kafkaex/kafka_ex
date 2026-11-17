defmodule KafkaExBench.ClusterTest do
  use ExUnit.Case, async: true
  alias KafkaExBench.{Cluster, Scenario}
  alias KafkaExBench.Scenarios.Basic

  test "the docker cluster has three brokers" do
    assert Cluster.expected_brokers() == 3
  end

  test "the scenario precheck and Basic follow the shared count" do
    assert Basic.cluster_problem(%{0 => 1}, %{0 => 1}, Cluster.expected_brokers()) == nil

    assert Basic.cluster_problem(%{0 => 1}, %{0 => 1}, Cluster.expected_brokers() - 1) =~
             "expected #{Cluster.expected_brokers()}"

    assert Basic.balanced?(%{0 => 1, 1 => 2, 2 => 3})
    Code.ensure_loaded!(Scenario)
    assert function_exported?(Scenario, :run_once, 4)
  end
end
