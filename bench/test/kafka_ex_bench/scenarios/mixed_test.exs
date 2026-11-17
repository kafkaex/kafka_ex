defmodule KafkaExBench.Scenarios.MixedTest do
  use ExUnit.Case, async: true
  alias KafkaExBench.Scenarios.Mixed
  alias KafkaExBench.Workers

  describe "longpoll_problem/2" do
    test "an empty answer after the wait is fine" do
      assert Mixed.longpoll_problem({:ok, %{records: []}}, 100) == nil
      assert Mixed.longpoll_problem({:ok, %{records: []}}, 50) == nil
    end

    test "an answer with records is reported as returned data" do
      assert Mixed.longpoll_problem({:ok, %{records: [%{}]}}, 100) =~ "returned data"
    end

    test "an empty answer faster than 50 ms is reported as returned early" do
      assert Mixed.longpoll_problem({:ok, %{records: []}}, 49) =~ "returned early"
    end

    test "an error is reported with its reason" do
      assert Mixed.longpoll_problem({:error, :timeout}, 100) =~ ":timeout"
    end
  end

  test "params record the long-poll load" do
    assert Mixed.params() == %{
             partitions: 6,
             replication_factor: 3,
             longpollers: 3,
             idle_partitions: 3,
             longpoll_max_wait_ms: 100,
             longpoll_max_bytes: 65_536,
             longpoll_min_wait_ms: 50,
             payload_bytes: 1024,
             acks: -1
           }
  end

  test "jobs carry a long-poll background that reports the ops it completed" do
    jobs = Mixed.jobs(self(), %{idle: "i", produce: "p", payload: "x"})

    assert Enum.all?(jobs, fn job -> job.background.stop == (&Workers.stop/1) end)
  end

  describe "idle topic placement" do
    test "broker 1, 2 and 3 lead partitions 0, 1 and 2" do
      assert Mixed.idle_replica_assignment() == [{0, [1, 2, 3]}, {1, [2, 3, 1]}, {2, [3, 1, 2]}]
      assert Mixed.idle_expected_layout() == %{0 => 1, 1 => 2, 2 => 3}
    end

    test "the expected layout is not a problem" do
      assert Mixed.idle_problem(%{0 => 1, 1 => 2, 2 => 3}) == nil
    end

    test "any other layout is reported with both" do
      message = Mixed.idle_problem(%{0 => 1, 1 => 1, 2 => 3})

      assert message =~ "idle topic leaders"
      assert message =~ inspect(%{0 => 1, 1 => 1, 2 => 3})
      assert message =~ inspect(%{0 => 1, 1 => 2, 2 => 3})
    end
  end

  test "layout reports both topics keyed by role" do
    ctx = %{layouts: %{idle: %{0 => 1}, produce: %{0 => 2}}}

    assert Mixed.layout(ctx) == %{produce: %{0 => 2}, idle: %{0 => 1}}
  end

  test "job specs list the two measured jobs at concurrency 1" do
    assert Mixed.job_specs() == [{"produce_under_longpoll", 1}, {"metadata_under_longpoll", 1}]
  end
end
