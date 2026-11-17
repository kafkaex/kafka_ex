defmodule KafkaExBench.Scenarios.RestartRecovery do
  @behaviour KafkaExBench.Scenario

  alias KafkaEx.API
  alias KafkaExBench.{Cluster, OneShotJob, Runner, Topics}

  @partitions 3
  @replication_factor 3
  @brokers Cluster.expected_brokers()
  @service "kafka-2"
  @restarted_broker 2
  @repetitions 3
  @steady_state_ms 5_000
  @recovery_bound_ms 120_000
  @ready_bound_ms 180_000
  @poll_ms 100
  @error_pause_ms 10
  @payload_bytes 1024
  @acks -1
  @overlay "bench/docker/compose.latency.yml"
  @bench_dir Path.expand("../../..", __DIR__)
  @restart_timeout_ms 120_000
  @job_specs [{"restart_kafka2", 1}]

  @impl true
  def job_specs, do: @job_specs

  @impl true
  def gates do
    %{
      "restart_kafka2" => [
        {:gap_other_leaders_us, :lower_better, {:usable_only}},
        {:gap_restarted_leader_us, :lower_better, {:usable_only}}
      ]
    }
  end

  # one-shot jobs ignore time and warmup
  @impl true
  def default_opts, do: [time: 0, warmup: 0, min_valid: 3]

  @impl true
  def setup(client, _profile) do
    {topic, layout} = create_topic(client, "restart-recovery")

    Topics.cleanup_on_failure(client, topic, fn ->
      %{topic: topic, layout: layout, payload: :crypto.strong_rand_bytes(@payload_bytes)}
    end)
  end

  @impl true
  def params do
    %{
      run_last: true,
      partitions: @partitions,
      replication_factor: @replication_factor,
      restarted_service: @service,
      repetitions: @repetitions,
      steady_state_ms: @steady_state_ms,
      recovery_bound_ms: @recovery_bound_ms,
      payload_bytes: @payload_bytes,
      acks: @acks
    }
  end

  @impl true
  def layout(ctx), do: ctx.layout

  @impl true
  def jobs(client, ctx) do
    [
      %OneShotJob{
        name: "restart_kafka2",
        repetitions: @repetitions,
        timeout_ms: 2 * @ready_bound_ms + @steady_state_ms + @recovery_bound_ms + 120_000,
        fun: fn -> repetition(client, ctx) end
      }
    ]
  end

  @impl true
  def check(client, %{topic: topic}), do: await_ready(client, topic, now_ms(), @ready_bound_ms)

  @impl true
  def teardown(client, %{topic: topic}) do
    Topics.delete!(client, topic)
    :ok
  end

  def restart_command(architecture, service) do
    {"docker", ["compose", "-f", compose_base(architecture), "-f", @overlay, "restart", service]}
  end

  def replica_assignment, do: [{0, [2, 1, 3]}, {1, [1, 2, 3]}, {2, [3, 1, 2]}]

  def expected_layout, do: Map.new(replica_assignment(), fn {partition, [leader | _]} -> {partition, leader} end)

  def repo_root(bench_dir), do: Path.expand("..", bench_dir)

  def restart_result({_output, 0}), do: :ok

  def restart_result({output, status}),
    do: {:error, "docker compose restart failed with status #{status}: #{Runner.sanitize_error(output)}"}

  def longest_gap(times) when length(times) < 2, do: 0

  def longest_gap(times) do
    times |> Enum.sort() |> Enum.chunk_every(2, 1, :discard) |> Enum.map(fn [a, b] -> b - a end) |> Enum.max()
  end

  def longest_gap_window(times) when length(times) < 2, do: {0, 0}

  def longest_gap_window(times) do
    times |> Enum.sort() |> Enum.chunk_every(2, 1, :discard) |> Enum.max_by(fn [a, b] -> b - a end) |> List.to_tuple()
  end

  def max_gap(events_by_partition),
    do: events_by_partition |> Map.values() |> Enum.map(&longest_gap/1) |> Enum.max(fn -> 0 end)

  def max_gap_window(events_by_partition) do
    events_by_partition
    |> Map.values()
    |> Enum.map(&longest_gap_window/1)
    |> Enum.max_by(fn {from, to} -> to - from end, fn -> {0, 0} end)
  end

  def class_gaps(events_by_partition, origin \\ 0) do
    restarted = for {partition, @restarted_broker} <- expected_layout(), do: partition
    {on_restarted, others} = Map.split(events_by_partition, restarted)

    Map.merge(
      class_entries(:gap_restarted_leader, on_restarted, origin),
      class_entries(:gap_other_leaders, others, origin)
    )
  end

  defp class_entries(prefix, events_by_partition, origin) do
    {from, to} = max_gap_window(events_by_partition)
    {from, to} = if from == to, do: {0, 0}, else: {from - origin, to - origin}

    %{
      :"#{prefix}_us" => [max_gap(events_by_partition) * 1_000],
      :"#{prefix}_start_ms" => [from],
      :"#{prefix}_end_ms" => [to]
    }
  end

  def recovery_status(last_success, restarted_at, now, bound_ms) do
    silent = for {partition, at} <- Enum.sort(last_success), is_nil(at) or at <= restarted_at, do: partition

    cond do
      silent == [] -> :recovered
      now - restarted_at >= bound_ms -> {:timeout, silent}
      true -> :waiting
    end
  end

  defp create_topic(client, prefix) do
    Topics.create_with_layout!(client, prefix, partitions: @partitions, replica_assignment: replica_assignment())
  end

  defp compose_base(architecture) do
    if String.starts_with?(architecture, ["aarch64", "arm64"]), do: "docker-compose-arm.yml", else: "docker-compose.yml"
  end

  defp repetition(client, %{topic: scenario_topic, payload: payload}) do
    Process.flag(:trap_exit, true)
    with {:error, message} <- await_ready(client, scenario_topic, now_ms(), @ready_bound_ms), do: raise(message)

    {topic, layout} = create_topic(client, "restart-recovery-rep")

    try do
      with {:error, message} <- await_ready(client, topic, now_ms(), @ready_bound_ms, expected_layout()),
           do: raise(message)

      measure_restart(client, topic, payload, layout)
    after
      Topics.delete_after_failure(client, topic)
    end
  end

  defp measure_restart(client, topic, payload, layout) do
    origin = now_ms()
    events = :ets.new(:restart_events, [:duplicate_bag, :public, write_concurrency: true])
    last = :ets.new(:restart_last, [:set, :public, write_concurrency: true])
    errors = :counters.new(1, [])

    loops =
      for partition <- 0..(@partitions - 1) do
        spawn_link(fn -> produce_loop(client, topic, partition, payload, events, last, errors) end)
      end

    outcome =
      try do
        Process.sleep(@steady_state_ms)

        command_started_at = now_ms()

        with :ok <- run_restart() do
          restarted_at = now_ms()

          with :recovered <- await_recovery(last, restarted_at),
               :ok <- await_rejoin(client, topic, restarted_at) do
            {:recovered, now_ms() - restarted_at, command_started_at, restarted_at}
          end
        end
      after
        Enum.each(loops, &Process.exit(&1, :kill))
      end

    case outcome do
      {:recovered, rejoin_ms, command_started_at, command_returned_at} ->
        by_partition = events_by_partition(events)

        extras = %{
          errors_during_repetitions: :counters.get(errors, 1),
          layouts: [layout],
          rejoin_ms: [rejoin_ms],
          restart_cmd_started_ms: [command_started_at - origin],
          restart_cmd_returned_ms: [command_returned_at - origin]
        }

        {:ok, max_gap(by_partition) * 1_000, Map.merge(extras, class_gaps(by_partition, origin))}

      {:timeout, partitions} ->
        {:error, "partitions #{inspect(partitions)} had no success within #{@recovery_bound_ms} ms after the restart"}

      {:error, _} = error ->
        error
    end
  end

  defp await_rejoin(client, topic, restarted_at) do
    case await_ready(client, topic, restarted_at, @recovery_bound_ms) do
      :ok -> :ok
      {:error, message} -> {:error, "after the restart: " <> message}
    end
  end

  defp run_restart do
    {command, args} = restart_command(to_string(:erlang.system_info(:system_architecture)), @service)

    task =
      Task.async(fn -> System.cmd(command, args, cd: repo_root(@bench_dir), stderr_to_stdout: true) end)

    case Task.yield(task, @restart_timeout_ms) || Task.shutdown(task, :brutal_kill) do
      {:ok, result} ->
        restart_result(result)

      {:exit, reason} ->
        {:error, "docker restart crashed: #{Runner.sanitize_error(reason)}"}

      nil ->
        {:error,
         "docker restart timed out after #{div(@restart_timeout_ms, 1000)} s (the docker CLI process may still be running)"}
    end
  end

  defp await_recovery(last, restarted_at) do
    last_success = Map.new(0..(@partitions - 1), fn partition -> {partition, last_success(last, partition)} end)

    case recovery_status(last_success, restarted_at, now_ms(), @recovery_bound_ms) do
      :waiting ->
        Process.sleep(@poll_ms)
        await_recovery(last, restarted_at)

      done ->
        done
    end
  end

  defp last_success(last, partition) do
    case :ets.lookup(last, partition) do
      [{^partition, at}] -> at
      [] -> nil
    end
  end

  defp events_by_partition(events) do
    Map.new(0..(@partitions - 1), fn partition ->
      {partition, for({_, at} <- :ets.lookup(events, partition), do: at)}
    end)
  end

  defp produce_loop(client, topic, partition, payload, events, last, errors) do
    case Runner.guarded(fn -> API.produce_one(client, topic, partition, payload, acks: @acks) end) do
      {:ok, {:ok, _}} ->
        at = now_ms()
        :ets.insert(events, {partition, at})
        :ets.insert(last, {partition, at})

      _ ->
        :counters.add(errors, 1, 1)
        Process.sleep(@error_pause_ms)
    end

    produce_loop(client, topic, partition, payload, events, last, errors)
  end

  def readiness_problem(broker_count, partitions, expected_layout \\ nil) do
    problems =
      [
        broker_count != @brokers && "#{broker_count} brokers, expected #{@brokers}",
        partitions == [] && "topic has no partitions",
        layout_problem(partitions, expected_layout),
        problem_list("partitions with no leader", for(p <- partitions, p.leader < 0, do: p.partition_id)),
        problem_list(
          "partitions with fewer than #{@brokers} replicas",
          for(p <- partitions, length(p.replicas) < @brokers, do: p.partition_id)
        )
      ] ++
        for p <- partitions, Enum.sort(p.isr) != Enum.sort(p.replicas) or length(p.isr) < @brokers do
          "partition #{p.partition_id} ISR #{inspect(p.isr)} is not all replicas #{inspect(p.replicas)}"
        end

    case Enum.filter(problems, & &1) do
      [] -> nil
      found -> Enum.join(found, "; ")
    end
  end

  defp layout_problem(_partitions, nil), do: nil

  defp layout_problem(partitions, expected) do
    current = Map.new(partitions, &{&1.partition_id, &1.leader})

    if current != expected, do: "leaders #{inspect(current)} differ from expected #{inspect(expected)}"
  end

  defp problem_list(_label, []), do: nil

  defp problem_list(label, ids), do: "#{label}: #{inspect(ids)}"

  def ready_error(bound_ms, problem), do: "cluster not ready after #{bound_ms} ms: #{problem}"

  defp await_ready(client, topic, since, bound_ms, expected \\ nil) do
    problem = current_problem(client, topic, expected)

    cond do
      is_nil(problem) ->
        :ok

      now_ms() >= since + bound_ms ->
        {:error, ready_error(bound_ms, problem)}

      true ->
        Process.sleep(500)
        await_ready(client, topic, since, bound_ms, expected)
    end
  end

  defp current_problem(client, topic, expected) do
    case API.metadata(client, [topic], []) do
      {:ok, %{topics: %{^topic => %{partitions: partitions}}} = metadata} ->
        readiness_problem(map_size(metadata.brokers), partitions, expected)

      {:ok, _} ->
        "topic #{topic} not in metadata"

      error ->
        "metadata failed: #{Runner.sanitize_error(error)}"
    end
  end

  defp now_ms, do: System.monotonic_time(:millisecond)
end
