defmodule KafkaExBench.Scenarios.Mixed do
  @behaviour KafkaExBench.Scenario

  alias KafkaEx.API
  alias KafkaExBench.Scenarios.Basic
  alias KafkaExBench.{Job, Scenario, Topics, Workers}

  @partitions 6
  @idle_partitions 3
  @replication_factor 3
  @longpollers 3
  @idle_replica_assignment [{0, [1, 2, 3]}, {1, [2, 3, 1]}, {2, [3, 1, 2]}]
  @longpoll_max_wait_ms 100
  @longpoll_min_wait_ms 50
  @longpoll_max_bytes 65_536
  @payload_bytes 1024
  @acks -1
  @min_samples 30
  @metrics [{:p50_us, :lower_better}, {:throughput_ops, :higher_better}]
  @job_specs [{"produce_under_longpoll", 1}, {"metadata_under_longpoll", 1}]

  @impl true
  def job_specs, do: @job_specs

  @impl true
  def gates, do: Scenario.steady_gates(@job_specs)

  @impl true
  def default_opts, do: [time: 30, warmup: 2, min_valid: 5]

  @impl true
  def setup(client, _profile) do
    {idle, idle_layout} =
      Topics.create_with_layout!(client, "mixed-idle",
        partitions: @idle_partitions,
        replica_assignment: @idle_replica_assignment
      )

    Topics.cleanup_on_failure(client, idle, fn ->
      {produce, produce_layout} = create_topic(client, "mixed-produce")

      Topics.cleanup_on_failure(client, produce, fn ->
        if problem = idle_problem(idle_layout), do: raise(problem)
        unless Basic.balanced?(produce_layout), do: raise("unbalanced leaders: #{inspect(produce_layout)}")

        %{
          idle: idle,
          produce: produce,
          payload: :crypto.strong_rand_bytes(@payload_bytes),
          layouts: %{idle: idle_layout, produce: produce_layout}
        }
      end)
    end)
  end

  @impl true
  def params do
    %{
      partitions: @partitions,
      replication_factor: @replication_factor,
      longpollers: @longpollers,
      idle_partitions: @idle_partitions,
      longpoll_max_wait_ms: @longpoll_max_wait_ms,
      longpoll_max_bytes: @longpoll_max_bytes,
      longpoll_min_wait_ms: @longpoll_min_wait_ms,
      payload_bytes: @payload_bytes,
      acks: @acks
    }
  end

  @impl true
  def layout(%{layouts: layouts}), do: %{produce: layouts.produce, idle: layouts.idle}

  @impl true
  def jobs(client, ctx) do
    background = %{start: fn -> start_longpollers(client, ctx.idle) end, stop: &Workers.stop/1}

    for {name, parallel} <- @job_specs do
      %Job{
        name: name,
        parallel: parallel,
        fun: job_fun(name, client, ctx),
        min_samples: @min_samples,
        metrics: @metrics,
        background: background
      }
    end
  end

  @impl true
  def check(client, %{idle: idle, produce: produce, layouts: layouts}) do
    {idle_current, brokers} = Topics.fetch_layout!(client, idle)
    {produce_current, _} = Topics.fetch_layout!(client, produce)

    case Basic.cluster_problem(layouts, %{idle: idle_current, produce: produce_current}, brokers) do
      nil -> :ok
      message -> {:error, message}
    end
  end

  @impl true
  def teardown(client, %{idle: idle, produce: produce}) do
    Topics.delete!(client, idle)
    Topics.delete!(client, produce)
    :ok
  end

  def idle_replica_assignment, do: @idle_replica_assignment

  def idle_expected_layout,
    do: Map.new(@idle_replica_assignment, fn {partition, [leader | _]} -> {partition, leader} end)

  def idle_problem(layout) do
    expected = idle_expected_layout()

    if layout != expected, do: "idle topic leaders #{inspect(layout)} differ from expected #{inspect(expected)}"
  end

  def longpoll_problem({:ok, %{records: [_ | _]}}, _elapsed_ms), do: "long-poll returned data"

  def longpoll_problem({:ok, %{records: []}}, elapsed_ms) when elapsed_ms < @longpoll_min_wait_ms,
    do: "long-poll returned early after #{elapsed_ms} ms"

  def longpoll_problem({:ok, %{records: []}}, _elapsed_ms), do: nil
  def longpoll_problem(other, _elapsed_ms), do: "long-poll failed: #{inspect(other)}"

  defp create_topic(client, prefix),
    do: Topics.create_with_layout!(client, prefix, partitions: @partitions, replication_factor: @replication_factor)

  defp start_longpollers(client, idle) do
    offsets =
      for partition <- 0..(@longpollers - 1) do
        {:ok, offset} = API.latest_offset(client, idle, partition)
        {partition, offset}
      end

    Workers.start(@longpollers, &Enum.at(offsets, &1), &longpoll(client, idle, &1))
  end

  defp longpoll(client, idle, {partition, offset} = state) do
    started = System.monotonic_time(:millisecond)

    result =
      API.fetch(client, idle, partition, offset, max_wait_time: @longpoll_max_wait_ms, max_bytes: @longpoll_max_bytes)

    case longpoll_problem(result, System.monotonic_time(:millisecond) - started) do
      nil -> {:ok, state}
      message -> {:error, message}
    end
  end

  defp job_fun("produce_under_longpoll", client, %{produce: topic, payload: payload}) do
    fn -> {:ok, _} = API.produce_one(client, topic, :rand.uniform(@partitions) - 1, payload, acks: @acks) end
  end

  defp job_fun("metadata_under_longpoll", client, %{produce: topic}) do
    fn -> {:ok, _} = API.metadata(client, [topic], []) end
  end
end
