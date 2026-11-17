defmodule KafkaExBench.Scenarios.OneBrokerTest do
  use ExUnit.Case, async: true
  alias KafkaExBench.Scenarios.OneBroker

  describe "replica_assignment/2" do
    test "every partition lists broker 1 first and all brokers once" do
      assignment = OneBroker.replica_assignment(6, 3)

      assert length(assignment) == 6
      assert Enum.map(assignment, &elem(&1, 0)) == [0, 1, 2, 3, 4, 5]

      for {_partition, replicas} <- assignment do
        assert hd(replicas) == 1
        assert Enum.sort(replicas) == [1, 2, 3]
      end
    end

    test "followers alternate between partitions so the followers are spread" do
      assert OneBroker.replica_assignment(4, 3) ==
               [{0, [1, 2, 3]}, {1, [1, 3, 2]}, {2, [1, 2, 3]}, {3, [1, 3, 2]}]
    end
  end

  describe "cluster_problem/2" do
    test "all leaders on broker 1 with three brokers is fine" do
      assert OneBroker.cluster_problem(%{0 => 1, 1 => 1, 2 => 1}, 3) == nil
    end

    test "a leader that moved is reported with the layout" do
      message = OneBroker.cluster_problem(%{0 => 1, 1 => 2, 2 => 1}, 3)

      assert message =~ "not all leaders on broker 1"
      assert message =~ inspect(%{0 => 1, 1 => 2, 2 => 1})
    end

    test "a leaderless partition is reported" do
      assert OneBroker.cluster_problem(%{0 => 1, 1 => -1}, 3) =~ "not all leaders on broker 1"
    end

    test "a missing broker is reported" do
      assert OneBroker.cluster_problem(%{0 => 1}, 2) == "2 brokers, expected 3"
    end
  end

  test "params describe the workload" do
    assert OneBroker.params() == %{
             partitions: 6,
             replication_factor: 3,
             leader_broker: 1,
             payload_bytes: 1024,
             acks: -1
           }
  end

  test "job specs list the spread and single partition produce jobs" do
    assert OneBroker.job_specs() == [{"produce_spread_c100", 100}, {"produce_single_partition_c100", 100}]
  end
end
