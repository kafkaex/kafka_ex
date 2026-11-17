defmodule KafkaExBench.Scenarios.LargeFetchTest do
  use ExUnit.Case, async: true
  alias KafkaExBench.Scenarios.LargeFetch
  alias KafkaExBench.Workers

  defp records(count, size), do: for(_ <- 1..count//1, do: %{value: :binary.copy("x", size)})

  describe "fetch_problem/1" do
    test "exactly 40 records is fine" do
      assert LargeFetch.fetch_problem({:ok, %{records: records(40, 102_400)}}) == nil
    end

    test "fewer records are reported with the count" do
      message = LargeFetch.fetch_problem({:ok, %{records: records(39, 102_400)}})

      assert message =~ "large fetch returned 39 records"
      assert message =~ "expected 40"
    end

    test "more records are reported too" do
      assert LargeFetch.fetch_problem({:ok, %{records: records(41, 10)}}) =~ "returned 41 records"
    end

    test "an empty fetch is reported" do
      assert LargeFetch.fetch_problem({:ok, %{records: []}}) =~ "returned 0 records"
    end

    test "an error is reported with its reason" do
      assert LargeFetch.fetch_problem({:error, :timeout}) =~ ":timeout"
    end
  end

  test "large preload batches stay below the broker message size limit" do
    %{large: %{batch_size: batch_size, batches_per_partition: batches, record_bytes: bytes}} = LargeFetch.preload_plan()

    assert batch_size * bytes < 1_000_000
    assert batch_size * batches == 40
  end

  test "layout reports both topics keyed by role" do
    ctx = %{layouts: %{large: %{0 => 1}, small: %{0 => 2}}}

    assert LargeFetch.layout(ctx) == %{small: %{0 => 2}, large: %{0 => 1}}
  end

  test "params describe both workloads" do
    assert LargeFetch.params() == %{
             large_partitions: 3,
             small_partitions: 6,
             replication_factor: 3,
             large_record_bytes: 102_400,
             large_batch_size: 5,
             large_records_per_partition: 40,
             large_fetch_workers: 4,
             large_fetch_max_bytes: 5_000_000,
             small_record_bytes: 1024,
             small_batch_size: 10,
             small_records_per_partition: 2_000,
             small_fetch_max_bytes: 65_536,
             small_fetch_max_wait_ms: 100,
             acks: -1
           }
  end

  test "large fetch jobs measure latency and throughput only, with the background load counted" do
    jobs = LargeFetch.jobs(self(), %{large: "l", small: "s", payload: "p"})

    assert Enum.all?(jobs, &(&1.metrics == [{:p50_us, :lower_better}, {:throughput_ops, :higher_better}]))
    assert Enum.all?(jobs, fn job -> job.background.stop == (&Workers.stop/1) end)
  end

  test "job specs list the small produce and fetch jobs at concurrency 10" do
    assert LargeFetch.job_specs() == [{"small_produce_c10", 10}, {"small_fetch_c10", 10}]
  end
end
