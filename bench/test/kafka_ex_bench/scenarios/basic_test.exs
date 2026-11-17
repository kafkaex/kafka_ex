defmodule KafkaExBench.Scenarios.BasicTest do
  use ExUnit.Case, async: true
  alias KafkaExBench.Scenarios.Basic

  test "two partitions per broker is balanced" do
    assert Basic.balanced?(%{0 => 1, 1 => 2, 2 => 3, 3 => 1, 4 => 2, 5 => 3})
  end

  test "a broker with three partitions is unbalanced" do
    refute Basic.balanced?(%{0 => 1, 1 => 1, 2 => 1, 3 => 2, 4 => 2, 5 => 3})
  end

  test "a broker with no partitions is unbalanced" do
    refute Basic.balanced?(%{0 => 1, 1 => 1, 2 => 2, 3 => 2, 4 => 1, 5 => 2})
  end

  test "a partition count that does not divide by the brokers is unbalanced" do
    refute Basic.balanced?(%{0 => 1, 1 => 2, 2 => 3, 3 => 1})
  end

  test "the broker count is a parameter" do
    assert Basic.balanced?(%{0 => 1, 1 => 2}, 2)
  end

  test "preload batches are small enough for max_bytes to bound a fetch" do
    %{batch_size: batch_size, batches_per_partition: batches} = Basic.preload_plan()

    assert batch_size * 1024 <= 65_536 / 4
    assert batch_size * batches == 2_000
  end

  describe "cluster_problem/3" do
    @layout %{0 => 1, 1 => 2, 2 => 3}

    test "an unchanged layout with three brokers is fine" do
      assert Basic.cluster_problem(@layout, @layout, 3) == nil
    end

    test "a moved leader is reported with both layouts" do
      moved = %{0 => 2, 1 => 2, 2 => 3}
      message = Basic.cluster_problem(@layout, moved, 3)

      assert message =~ "leaders changed"
      assert message =~ inspect(@layout)
      assert message =~ inspect(moved)
    end

    test "a missing broker is reported" do
      assert Basic.cluster_problem(@layout, @layout, 2) == "2 brokers, expected 3"
    end
  end

  test "params describe the workload" do
    assert Basic.params() == %{
             partitions: 6,
             replication_factor: 3,
             records_per_partition: 2_000,
             batch_size: 10,
             payload_bytes: 1024,
             fetch_max_bytes: 65_536,
             fetch_max_wait_ms: 100,
             acks: -1
           }
  end

  test "job specs list the six jobs with their concurrency" do
    assert Basic.job_specs() == [
             {"produce_c1", 1},
             {"produce_c10", 10},
             {"produce_c100", 100},
             {"fetch_c1", 1},
             {"fetch_c10", 10},
             {"fetch_c100", 100}
           ]
  end
end
