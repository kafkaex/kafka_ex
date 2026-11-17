defmodule KafkaExBench.Scenarios.RestartRecoveryTest do
  use ExUnit.Case, async: true
  alias KafkaExBench.Scenarios.RestartRecovery

  describe "restart_command/2" do
    test "uses the arm base file on arm64 and aarch64" do
      for arch <- ["aarch64-apple-darwin25.5.0", "arm64-apple-darwin", "aarch64-unknown-linux-gnu"] do
        assert {"docker", ["compose", "-f", "docker-compose-arm.yml" | _]} =
                 RestartRecovery.restart_command(arch, "kafka-2")
      end
    end

    test "uses the default base file elsewhere" do
      assert {"docker", ["compose", "-f", "docker-compose.yml" | _]} =
               RestartRecovery.restart_command("x86_64-pc-linux-gnu", "kafka-2")
    end

    test "layers the latency overlay and restarts the named service, all as separate list elements" do
      assert RestartRecovery.restart_command("x86_64-pc-linux-gnu", "kafka-2") ==
               {"docker",
                [
                  "compose",
                  "-f",
                  "docker-compose.yml",
                  "-f",
                  "bench/docker/compose.latency.yml",
                  "restart",
                  "kafka-2"
                ]}
    end
  end

  describe "repo_root/1" do
    test "is the parent of the bench project directory" do
      assert RestartRecovery.repo_root("/work/kafka_ex/bench") == "/work/kafka_ex"
    end
  end

  describe "restart_result/1" do
    test "exit status 0 is ok" do
      assert RestartRecovery.restart_result({"restarted", 0}) == :ok
    end

    test "a non-zero status fails with the status and the output" do
      assert {:error, message} = RestartRecovery.restart_result({"no such service: kafka-2\n", 1})
      assert message =~ "docker compose restart failed with status 1"
      assert message =~ "no such service: kafka-2"
    end
  end

  describe "longest_gap/1" do
    test "is the largest difference between consecutive successes regardless of order" do
      assert RestartRecovery.longest_gap([100, 105, 3_000, 3_004, 110]) == 2_890
    end

    test "fewer than two successes has no gap" do
      assert RestartRecovery.longest_gap([]) == 0
      assert RestartRecovery.longest_gap([42]) == 0
    end
  end

  describe "longest_gap_window/1" do
    test "is the start and end of the largest gap" do
      assert RestartRecovery.longest_gap_window([100, 105, 3_000, 3_004, 110]) == {110, 3_000}
    end

    test "fewer than two successes has an empty window at zero" do
      assert RestartRecovery.longest_gap_window([42]) == {0, 0}
    end
  end

  describe "max_gap/1" do
    test "is the largest gap over partitions" do
      events = %{0 => [0, 10, 20], 1 => [0, 500, 510], 2 => [0, 30]}

      assert RestartRecovery.max_gap(events) == 500
    end
  end

  test "ready_error names the bound and the problem" do
    assert RestartRecovery.ready_error(120_000, "2 brokers, expected 3") ==
             "cluster not ready after 120000 ms: 2 brokers, expected 3"
  end

  describe "class_gaps/1" do
    test "splits the longest gaps into the restarted leader and the other leaders, in microseconds" do
      events = %{0 => [0, 10, 1_500], 1 => [0, 40, 80], 2 => [0, 300]}

      assert %{gap_restarted_leader_us: [1_490_000], gap_other_leaders_us: [300_000]} =
               RestartRecovery.class_gaps(events)
    end

    test "the restarted leader can be the smaller one" do
      events = %{0 => [0, 5], 1 => [0, 900], 2 => [0, 100]}

      assert %{gap_restarted_leader_us: [5_000], gap_other_leaders_us: [900_000]} = RestartRecovery.class_gaps(events)
    end

    test "reports where the longest gap of each class starts and ends, as offsets from the origin" do
      events = %{0 => [1_000, 1_010, 2_500], 1 => [1_000, 1_040, 1_080], 2 => [1_000, 1_300]}

      assert RestartRecovery.class_gaps(events, 1_000) == %{
               gap_restarted_leader_us: [1_490_000],
               gap_restarted_leader_start_ms: [10],
               gap_restarted_leader_end_ms: [1_500],
               gap_other_leaders_us: [300_000],
               gap_other_leaders_start_ms: [0],
               gap_other_leaders_end_ms: [300]
             }
    end

    test "a class without a gap has a zero window" do
      events = %{0 => [], 1 => [5], 2 => []}

      assert %{gap_restarted_leader_start_ms: [0], gap_restarted_leader_end_ms: [0]} =
               RestartRecovery.class_gaps(events, 100)
    end

    test "the overall maximum equals the larger of the two classes" do
      events = %{0 => [0, 10, 1_500], 1 => [0, 40, 80], 2 => [0, 300]}
      %{gap_restarted_leader_us: [a], gap_other_leaders_us: [b]} = RestartRecovery.class_gaps(events)

      assert max(a, b) == RestartRecovery.max_gap(events) * 1_000
    end
  end

  describe "recovery_status/4" do
    @restarted_at 1_000

    test "recovered once every partition has a success after the restart returned" do
      assert RestartRecovery.recovery_status(%{0 => 1_050, 1 => 1_200}, @restarted_at, 1_300, 120_000) == :recovered
    end

    test "a success before the restart returned does not count" do
      assert RestartRecovery.recovery_status(%{0 => 1_050, 1 => 900}, @restarted_at, 1_300, 120_000) == :waiting
    end

    test "a partition without any success is waited for" do
      assert RestartRecovery.recovery_status(%{0 => 1_050, 1 => nil}, @restarted_at, 1_300, 120_000) == :waiting
    end

    test "times out and names the partitions still silent once the bound has passed" do
      assert RestartRecovery.recovery_status(%{0 => 5_000, 1 => 900, 2 => nil}, @restarted_at, 121_000, 120_000) ==
               {:timeout, [1, 2]}
    end

    test "recovery at the very end of the bound still wins over the timeout" do
      assert RestartRecovery.recovery_status(%{0 => 121_000}, @restarted_at, 121_000, 120_000) == :recovered
    end
  end

  describe "params/0" do
    test "no longer claims to be direct-only" do
      refute Map.has_key?(RestartRecovery.params(), :direct_only)
    end

    test "says it runs last" do
      assert RestartRecovery.params().run_last == true
    end

    test "has the single restart job" do
      assert RestartRecovery.job_specs() == [{"restart_kafka2", 1}]
    end
  end

  describe "readiness_problem/2" do
    defp partition(id, leader, isr), do: %{partition_id: id, leader: leader, replicas: [1, 2, 3], isr: isr}

    defp healthy, do: [partition(0, 1, [1, 2, 3]), partition(1, 2, [2, 1, 3]), partition(2, 3, [3, 1, 2])]

    test "three brokers, leaders everywhere and full ISRs is ready" do
      assert RestartRecovery.readiness_problem(3, healthy()) == nil
    end

    test "a missing broker is named" do
      assert RestartRecovery.readiness_problem(2, healthy()) =~ "2 brokers, expected 3"
    end

    test "a leaderless partition is named" do
      problem = RestartRecovery.readiness_problem(3, [partition(0, 1, [1, 2, 3]), partition(1, -1, [1, 2, 3])])

      assert problem =~ "no leader"
      assert problem =~ "1"
    end

    test "a shrunken ISR is named with the partition and its ISR" do
      problem = RestartRecovery.readiness_problem(3, [partition(0, 1, [1, 3]), partition(1, 2, [1, 2, 3])])

      assert problem =~ "ISR"
      assert problem =~ "partition 0"
      assert problem =~ "[1, 3]"
    end

    test "fewer replicas than brokers is not ready even with a full ISR" do
      assert RestartRecovery.readiness_problem(3, [%{partition_id: 0, leader: 1, replicas: [1, 2], isr: [1, 2]}]) =~
               "replicas"
    end

    test "a topic with no partitions yet is not ready" do
      assert RestartRecovery.readiness_problem(3, []) =~ "no partitions"
    end

    test "problems are reported together" do
      problem = RestartRecovery.readiness_problem(2, [partition(0, -1, [2])])

      assert problem =~ "2 brokers"
      assert problem =~ "no leader"
      assert problem =~ "ISR"
    end
  end

  describe "replica_assignment/0" do
    test "broker 2 is the preferred leader of exactly partition 0" do
      assert RestartRecovery.replica_assignment() == [{0, [2, 1, 3]}, {1, [1, 2, 3]}, {2, [3, 1, 2]}]
    end

    test "expected_layout/0 is the first replica of every partition" do
      assert RestartRecovery.expected_layout() == %{0 => 2, 1 => 1, 2 => 3}
    end
  end

  describe "readiness_problem/3 with an expected layout" do
    defp led(id, leader), do: %{partition_id: id, leader: leader, replicas: [1, 2, 3], isr: [1, 2, 3]}

    test "the expected leaders are ready" do
      partitions = [led(0, 2), led(1, 1), led(2, 3)]

      assert RestartRecovery.readiness_problem(3, partitions, %{0 => 2, 1 => 1, 2 => 3}) == nil
    end

    test "a partition led by someone else is named with both layouts" do
      partitions = [led(0, 1), led(1, 1), led(2, 3)]
      problem = RestartRecovery.readiness_problem(3, partitions, %{0 => 2, 1 => 1, 2 => 3})

      assert problem =~ "leaders"
      assert problem =~ inspect(%{0 => 1, 1 => 1, 2 => 3})
      assert problem =~ inspect(%{0 => 2, 1 => 1, 2 => 3})
    end

    test "without an expected layout any leaders are fine" do
      assert RestartRecovery.readiness_problem(3, [led(0, 1), led(1, 1), led(2, 1)]) == nil
    end
  end
end
