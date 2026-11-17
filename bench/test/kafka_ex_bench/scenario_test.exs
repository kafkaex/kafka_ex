defmodule KafkaExBench.ScenarioTest do
  use ExUnit.Case, async: false
  import ExUnit.CaptureIO
  import ExUnit.CaptureLog
  alias KafkaExBench.{Job, OneShotJob, Scenario}

  defmodule Fake do
    @behaviour KafkaExBench.Scenario

    def job_specs, do: [{"a", 1}, {"b", 2}]
    def gates, do: %{}
    def default_opts, do: [time: 1, warmup: 0]

    def setup(client, _profile) do
      record(:setup)
      if Process.get(:kill_client_in_setup), do: Process.exit(client, :kill)
      if Process.get(:fail_setup), do: raise("setup-marker")
      %{layout: %{0 => 1}}
    end

    def params, do: %{fake: true}

    def layout(ctx), do: ctx.layout

    def jobs(client, _ctx) do
      record(:jobs)
      if Process.get(:raise_in_jobs), do: raise("jobs-marker")
      kill = Process.get(:kill_client_in_job)

      for {name, parallel} <- job_specs() do
        fun =
          cond do
            Process.get(:fail_job) == name -> fn -> raise "job-marker" end
            kill -> fn -> Process.exit(client, :kill) end
            true -> fn -> :ok end
          end

        %Job{name: name, parallel: parallel, fun: fun}
      end
    end

    def check(_client, _ctx) do
      record(:check)
      if Process.get(:check_raise), do: raise("check-marker")

      case Process.get(:check_fail_after) do
        nil -> :ok
        n -> if length(Enum.filter(Process.get(:steps), &(&1 == :check))) >= n, do: {:error, "leader moved"}, else: :ok
      end
    end

    def teardown(client, _ctx) do
      record(:teardown)
      if Process.get(:kill_client_in_teardown), do: Process.exit(client, :kill)
      if Process.get(:hang_client_in_teardown), do: Agent.cast(client, fn _ -> Process.sleep(:infinity) end)
      if Process.get(:fail_teardown), do: raise("teardown-marker")
      :ok
    end

    defp record(step), do: Process.put(:steps, [step | Process.get(:steps, [])])
  end

  defmodule Mixed do
    @behaviour KafkaExBench.Scenario

    def job_specs, do: [{"shot", 1}, {"loop", 2}, {"shot2", 1}]
    def gates, do: %{}
    def default_opts, do: [time: 1, warmup: 0]
    def setup(_client, _profile), do: %{layout: %{}}
    def params, do: %{}
    def layout(ctx), do: ctx.layout

    def jobs(_client, _ctx) do
      shot_result = Process.get(:shot_result, {:ok, 12})

      [
        %OneShotJob{name: "shot", repetitions: 3, fun: fn -> shot_result end},
        %Job{name: "loop", parallel: 2, fun: fn -> :ok end, metrics: [{:p99_us, :lower_better}]},
        %OneShotJob{name: "shot2", repetitions: 2, fun: fn -> {:ok, 5} end}
      ]
    end

    def check(_client, _ctx) do
      Process.put(:checks, Process.get(:checks, 0) + 1)
      if Process.get(:check_error), do: {:error, "leader moved"}, else: :ok
    end

    def teardown(_client, _ctx), do: :ok
  end

  defp opts(extra \\ []),
    do:
      Keyword.merge(
        [warmup: 0, time: 0.2, min_samples: 1, prepare: fn _ -> :ok end, cluster_info: &healthy_cluster/1],
        extra
      )

  defp healthy_cluster(_client), do: %{brokers: 3, topic_count: 4, leaked_bench_topics: 1}

  defp steps, do: Enum.reverse(Process.get(:steps, []))

  defp run(extra \\ []) do
    capture_log(fn ->
      capture_io(:stderr, fn -> send(self(), {:result, Scenario.run_once(Fake, self(), :direct, opts(extra))}) end)
    end)

    assert_received {:result, result}
    result
  end

  test "a healthy run returns the layout and a valid record per job" do
    result = run()

    assert result.layout == %{0 => 1}
    assert Enum.map(result.jobs, & &1.job) == ["a", "b"]
    assert Enum.all?(result.jobs, & &1.valid)
    assert steps() == [:setup, :jobs, :check, :check, :teardown]
  end

  test "records carry the min_samples applied, skipped and failed ones carry none" do
    assert Enum.all?(run().jobs, &(&1.min_samples == 1))

    Process.put(:fail_job, "a")
    assert Enum.all?(run().jobs, &(&1.min_samples in [1, nil]))

    assert Enum.all?(
             run(cluster_info: fn _ -> %{brokers: 2, topic_count: 0, leaked_bench_topics: 0} end).jobs,
             &(&1.min_samples == nil)
           )
  end

  test "every record carries a metrics list, also failed and skipped ones" do
    Process.put(:fail_job, "a")

    assert Enum.all?(run().jobs, &match?([{:throughput_ops, :higher_better} | _], &1.metrics))

    assert Enum.all?(
             run(cluster_info: fn _ -> %{brokers: 2, topic_count: 0, leaked_bench_topics: 0} end).jobs,
             &is_list(&1.metrics)
           )
  end

  describe "one-shot jobs" do
    defp run_mixed(extra \\ []) do
      capture_log(fn ->
        capture_io(:stderr, fn ->
          send(self(), {:result, Scenario.run_once(Mixed, self(), :direct, opts(extra))})
        end)
      end)

      assert_received {:result, result}
      result
    end

    test "are dispatched by struct next to benchee jobs, with check after each" do
      result = run_mixed()

      assert [%{job: "shot", p50_us: 12, samples: 3}, %{job: "loop", parallel: 2}, %{job: "shot2", p50_us: 5}] =
               result.jobs

      assert Enum.all?(result.jobs, &(&1.valid and is_binary(&1.started_at)))
      assert Enum.at(result.jobs, 0).metrics == [{:p50_us, :lower_better}]
      assert Enum.at(result.jobs, 1).metrics == [{:p99_us, :lower_better}]
      assert Process.get(:checks) == 3
    end

    test "a failing one-shot skips the rest and skips the check" do
      Process.put(:shot_result, {:error, :no_way})
      result = run_mixed()

      assert [%{job: "shot", valid: false, error: ":no_way"}, skipped, skipped2] = result.jobs
      assert skipped.error == "skipped after shot failed"
      assert skipped2.error == "skipped after shot failed"
      assert skipped2.metrics == [{:p50_us, :lower_better}]
      assert Process.get(:checks) == nil
    end

    test "check runs after a valid one-shot and can invalidate it" do
      Process.put(:check_error, true)
      result = run_mixed()

      assert [%{job: "shot", valid: false, error: "cluster changed: leader moved", metrics: [{:p50_us, _}]} | _] =
               result.jobs
    end
  end

  test "on_job receives each job record as soon as it finishes" do
    parent = self()
    run(on_job: fn record -> send(parent, {:job, record.job, Process.get(:steps)}) end)

    assert_received {:job, "a", [:check, :jobs, :setup]}
    assert_received {:job, "b", [:check, :check, :jobs, :setup]}
  end

  test "on_job also receives skipped jobs" do
    Process.put(:fail_job, "a")
    parent = self()
    run(on_job: fn record -> send(parent, {:job, record.job, record.valid}) end)

    assert_received {:job, "a", false}
    assert_received {:job, "b", false}
  end

  test "on_job receives the invalid records of a failed setup" do
    Process.put(:fail_setup, true)
    parent = self()
    run(on_job: fn record -> send(parent, {:job, record.job, record.error}) end)

    assert_received {:job, "a", error}
    assert error =~ "setup-marker"
    assert_received {:job, "b", _}
  end

  test "the run records the topic counts from the precheck" do
    result = run()

    assert %{cluster_topic_count: 4, leaked_bench_topics: 1} = result
  end

  test "an unhealthy cluster invalidates every job before setup" do
    result = run(cluster_info: fn _ -> %{brokers: 2, topic_count: 7, leaked_bench_topics: 3} end)

    assert Enum.all?(result.jobs, &(not &1.valid and &1.error == "cluster unhealthy: 2 brokers"))
    assert %{layout: nil, cluster_topic_count: 7, leaked_bench_topics: 3} = result
    assert steps() == []
  end

  test "a failing precheck invalidates every job before setup" do
    result = run(cluster_info: fn _ -> raise "metadata-marker" end)

    assert Enum.all?(result.jobs, &(not &1.valid and &1.error =~ "metadata-marker"))
    assert %{cluster_topic_count: nil, leaked_bench_topics: nil} = result
    assert steps() == []
  end

  test "leaked_count counts only bench- topics" do
    assert Scenario.leaked_count(["bench-basic-1", "orders", "bench-x", "my-bench-y"]) == 2
  end

  test "every job record carries its start time and duration" do
    result = run()

    for job <- result.jobs do
      assert {:ok, _, 0} = DateTime.from_iso8601(job.started_at)
      assert is_integer(job.duration_ms) and job.duration_ms >= 0
    end
  end

  test "skipped jobs also carry timestamps" do
    Process.put(:fail_job, "a")
    result = run()

    assert Enum.all?(result.jobs, &(is_binary(&1.started_at) and is_integer(&1.duration_ms)))
  end

  describe "whole-run invalidation after jobs were reported" do
    defp collect(extra) do
      parent = self()

      run(
        [
          on_job: fn record -> send(parent, {:job, record.job, record.valid}) end,
          on_invalidated: fn message -> send(parent, {:invalidated, message}) end
        ] ++ extra
      )
    end

    test "a teardown failure reports one invalidation line instead of re-emitting the jobs" do
      Process.put(:fail_teardown, true)
      result = collect([])

      assert_received {:job, "a", true}
      assert_received {:job, "b", true}
      refute_received {:job, _, _}
      assert_received {:invalidated, message}
      assert message =~ "teardown failed" and message =~ "teardown-marker"
      assert Enum.all?(result.jobs, &(not &1.valid))
    end

    test "a failure while measuring reports one invalidation line" do
      Process.put(:raise_in_jobs, true)
      result = collect([])

      refute_received {:job, _, _}
      assert_received {:invalidated, message}
      assert message =~ "jobs-marker"
      assert Enum.all?(result.jobs, &(not &1.valid))
    end

    test "a client exit reports one invalidation line" do
      Process.put(:kill_client_in_job, true)
      parent = self()

      capture_log(fn ->
        capture_io(:stderr, fn ->
          result =
            Scenario.run_with_client(
              Fake,
              :direct,
              opts(
                start_client: &start_fake/0,
                on_job: fn record -> send(parent, {:job, record.job}) end,
                on_invalidated: fn message -> send(parent, {:invalidated, message}) end
              )
            )

          send(parent, {:result, result})
        end)
      end)

      assert_received {:result, result}
      assert_received {:invalidated, message}
      assert message =~ "client exited"
      assert Enum.all?(result.jobs, &(not &1.valid and &1.error =~ "client exited"))
    end
  end

  test "a failing setup marks every job invalid and does not tear down" do
    Process.put(:fail_setup, true)
    result = run()

    assert result.layout == nil
    assert Enum.map(result.jobs, &{&1.job, &1.parallel, &1.valid}) == [{"a", 1, false}, {"b", 2, false}]
    assert Enum.all?(result.jobs, &(&1.error =~ "setup-marker"))
    assert steps() == [:setup]
  end

  test "a failing prepare marks every job invalid before setup" do
    result = run(prepare: fn _ -> raise "prepare-marker" end)

    assert Enum.all?(result.jobs, &(not &1.valid and &1.error =~ "prepare-marker"))
    assert steps() == []
  end

  test "the jobs after an invalid one are skipped and teardown still runs" do
    Process.put(:fail_job, "a")
    result = run()

    assert [%{job: "a", valid: false}, skipped] = result.jobs
    assert %{job: "b", parallel: 2, valid: false, error: "skipped after a failed", samples: 0} = skipped
    assert skipped.throughput_ops == nil and skipped.vm_cpu_us_per_op == nil
    assert steps() == [:setup, :jobs, :teardown]
  end

  test "a failing check invalidates that job and skips the rest" do
    Process.put(:check_fail_after, 1)
    result = run()

    assert [%{job: "a", valid: false, error: "cluster changed: leader moved"}, skipped] = result.jobs
    assert %{job: "b", valid: false, error: "skipped after a failed"} = skipped
    assert steps() == [:setup, :jobs, :check, :teardown]
  end

  test "a check after a valid job keeps earlier jobs valid" do
    Process.put(:check_fail_after, 2)
    result = run()

    assert [%{job: "a", valid: true}, %{job: "b", valid: false, error: "cluster changed: leader moved"}] = result.jobs
  end

  test "a raising check invalidates the job" do
    Process.put(:check_raise, true)
    result = run()

    assert [%{valid: false, error: "cluster changed: " <> detail}, %{valid: false}] = result.jobs
    assert detail =~ "check-marker"
  end

  test "an invalid job is not checked" do
    Process.put(:fail_job, "a")
    run()

    refute :check in steps()
  end

  test "jobs before the invalid one keep their valid records" do
    Process.put(:fail_job, "b")
    result = run()

    assert [%{job: "a", valid: true}, %{job: "b", valid: false, error: error}] = result.jobs
    assert error =~ "job-marker"
  end

  test "a teardown failure after a successful run marks every job invalid" do
    Process.put(:fail_teardown, true)
    result = run()

    assert Enum.all?(result.jobs, &(not &1.valid and &1.error =~ "teardown failed" and &1.error =~ "teardown-marker"))
  end

  test "a teardown failure after a failed run only warns and keeps the original error" do
    Process.put(:fail_teardown, true)
    Process.put(:fail_job, "a")

    warning =
      capture_io(:stderr, fn ->
        capture_log(fn -> send(self(), {:result, Scenario.run_once(Fake, self(), :direct, opts())}) end)
      end)

    assert_received {:result, result}

    assert warning =~ "teardown-marker"
    assert [%{valid: false, error: error}, %{valid: false, error: "skipped after a failed"}] = result.jobs
    assert error =~ "job-marker"
  end

  describe "run_with_client/3" do
    defp start_fake do
      Agent.start_link(fn -> nil end)
    end

    defp supervised(extra \\ []) do
      capture_log(fn ->
        capture_io(:stderr, fn ->
          result = Scenario.run_with_client(Fake, :direct, opts(Keyword.merge([start_client: &start_fake/0], extra)))
          send(self(), {:result, result})
        end)
      end)

      assert_received {:result, result}
      result
    end

    test "a client that dies abnormally during measurement invalidates the run and spares the caller" do
      Process.put(:kill_client_in_job, true)
      result = supervised()

      assert Enum.all?(result.jobs, &(not &1.valid and &1.error =~ "client exited" and &1.error =~ ":killed"))
      assert Process.alive?(self())
    end

    test "a client that dies abnormally during setup invalidates the run" do
      Process.put(:kill_client_in_setup, true)
      result = supervised()

      assert Enum.all?(result.jobs, &(not &1.valid and &1.error =~ "client exited"))
    end

    test "a client that dies abnormally during teardown invalidates the run" do
      Process.put(:kill_client_in_teardown, true)
      result = supervised()

      assert Enum.all?(result.jobs, &(not &1.valid and &1.error =~ "client exited"))
    end

    test "a healthy client yields valid jobs and the trap_exit flag is restored" do
      refute Process.flag(:trap_exit, false)
      result = supervised()

      assert Enum.all?(result.jobs, & &1.valid)
      refute Process.info(self(), :trap_exit) == {:trap_exit, true}
    end

    test "a client that does not stop is killed and the run is invalidated" do
      Process.put(:hang_client_in_teardown, true)
      test_pid = self()

      start = fn ->
        {:ok, pid} = start_fake()
        send(test_pid, {:client, pid})
        {:ok, pid}
      end

      result = supervised(start_client: start, stop_timeout: 100)

      assert_received {:client, client}
      refute Process.alive?(client)
      assert Enum.all?(result.jobs, &(not &1.valid and &1.error =~ "client exited" and &1.error =~ ":killed"))
    end

    test "a failing client start marks every job invalid" do
      result =
        capture_log(fn ->
          send(
            self(),
            {:result, Scenario.run_with_client(Fake, :direct, opts(start_client: fn -> {:error, :econnrefused} end))}
          )
        end)

      assert is_binary(result)
      assert_received {:result, run}
      assert Enum.all?(run.jobs, &(not &1.valid and &1.error =~ "client start failed"))
      assert %{cluster_topic_count: nil, leaked_bench_topics: nil} = run
    end

    defp start_error(start_client) do
      capture_log(fn ->
        send(self(), {:result, Scenario.run_with_client(Fake, :direct, opts(start_client: start_client))})
      end)

      assert_received {:result, run}
      hd(run.jobs).error
    end

    test "an error tuple from the client start is shown as data, not as an Erlang error" do
      assert start_error(fn -> {:error, :no_broker} end) == "client start failed: {:error, :no_broker}"
    end

    test "an exit while starting the client is shown as an exit banner" do
      assert start_error(fn -> exit(:boom) end) == "client start failed: ** (exit) :boom"
    end

    test "a throw while starting the client is shown as a throw banner" do
      assert start_error(fn -> throw(:boom) end) == "client start failed: ** (throw) :boom"
    end
  end
end
