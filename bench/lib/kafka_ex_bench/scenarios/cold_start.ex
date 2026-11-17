defmodule KafkaExBench.Scenarios.ColdStart do
  @behaviour KafkaExBench.Scenario

  alias KafkaEx.API
  alias KafkaExBench.{Cluster, OneShotJob, Profile, Runner, Topics}

  @partition_counts [1, 10, 100]
  @replication_factor 3
  @repetitions 15
  @discarded_repetitions 1
  @brokers Cluster.expected_brokers()
  @ready_deadline_ms 30_000
  @retry_pause_ms 20
  @stop_timeout_ms 10_000
  @job_specs for p <- @partition_counts, do: {"cold_start_p#{p}", 1}

  @impl true
  def job_specs, do: @job_specs

  @impl true
  def gates, do: Map.new(@job_specs, fn {name, _parallel} -> {name, [{:p50_us, :lower_better, {:relative}}]} end)

  # one-shot jobs ignore time and warmup
  @impl true
  def default_opts, do: [time: 0, warmup: 0, min_valid: 5]

  @impl true
  def setup(client, profile) do
    topics = create_topics(client, @partition_counts, [])
    %{topics: topics, profile: profile, layouts: Map.new(topics, fn {p, _topic, layout} -> {p, layout} end)}
  end

  @impl true
  def params do
    %{
      partition_counts: @partition_counts,
      replication_factor: @replication_factor,
      repetitions: @repetitions,
      discarded_repetitions: @discarded_repetitions,
      fetch_max_wait_ms: 0,
      security: :scram
    }
  end

  @impl true
  def layout(ctx), do: ctx.layouts[List.last(@partition_counts)]

  @impl true
  def jobs(_client, %{topics: topics, profile: profile}) do
    for {partitions, topic, _layout} <- topics do
      %OneShotJob{
        name: "cold_start_p#{partitions}",
        repetitions: @repetitions,
        discard: @discarded_repetitions,
        fun: fn -> cold_start(profile, topic, partitions) end
      }
    end
  end

  @impl true
  def check(client, %{topics: topics}) do
    Enum.find_value(topics, :ok, fn {_p, topic, layout} ->
      {current, brokers} = Topics.fetch_layout!(client, topic)

      case problem(layout, current, brokers) do
        nil -> nil
        message -> {:error, message}
      end
    end)
  end

  @impl true
  def teardown(client, %{topics: topics}) do
    Enum.each(topics, fn {_p, topic, _layout} -> Topics.delete!(client, topic) end)
    :ok
  end

  def evenly_spread?(layout, brokers) do
    partitions = map_size(layout)
    leaders = Map.values(layout)
    counts = Enum.frequencies(leaders)
    low = div(partitions, brokers)
    high = if rem(partitions, brokers) == 0, do: low, else: low + 1

    Enum.all?(leaders, &(&1 >= 0)) and
      Enum.all?(counts, fn {_leader, count} -> count in low..high end) and
      (partitions < brokers or map_size(counts) == brokers)
  end

  def elapsed_us(started_us, success_times_us), do: Enum.max(success_times_us) - started_us

  defp problem(layout, current, brokers) do
    cond do
      brokers != @brokers -> "#{brokers} brokers, expected #{@brokers}"
      current != layout -> "leaders changed from #{inspect(layout)} to #{inspect(current)}"
      true -> nil
    end
  end

  defp create_topics(_client, [], acc), do: Enum.reverse(acc)

  defp create_topics(client, [partitions | rest], acc) do
    {topic, layout} =
      Topics.create_with_layout!(client, "cold-start-p#{partitions}",
        partitions: partitions,
        replication_factor: @replication_factor
      )

    Topics.cleanup_on_failure(client, topic, fn ->
      unless evenly_spread?(layout, @brokers), do: raise("unbalanced leaders: #{inspect(layout)}")
      create_topics(client, rest, [{partitions, topic, layout} | acc])
    end)
  end

  defp cold_start(profile, topic, partitions) do
    Process.flag(:trap_exit, true)
    started = System.monotonic_time(:microsecond)

    case API.start_client(Profile.client_opts(profile, :scram)) do
      {:ok, client} ->
        try do
          fetch_every_partition(client, topic, partitions, started)
        after
          stop(client)
        end

      other ->
        {:error, "client start failed: #{Runner.sanitize_error(other)}"}
    end
  end

  defp fetch_every_partition(client, topic, partitions, started) do
    deadline = System.monotonic_time(:millisecond) + @ready_deadline_ms

    results =
      0..(partitions - 1)
      |> Task.async_stream(&first_success(fn -> API.fetch(client, topic, &1, 0, max_wait_time: 0) end, &1, deadline),
        max_concurrency: partitions,
        timeout: :infinity,
        ordered: false
      )
      |> Enum.map(fn {:ok, result} -> result end)

    case Enum.find(results, &match?({:error, _}, &1)) do
      nil ->
        {:ok, elapsed_us(started, for({:ok, at, _} <- results, do: at)),
         %{fetch_retries: Enum.sum(for {:ok, _, retries} <- results, do: retries)}}

      error ->
        error
    end
  end

  def first_success(fetch, partition, deadline, retries \\ 0) do
    case Runner.guarded(fetch) do
      {:ok, {:ok, _}} ->
        {:ok, System.monotonic_time(:microsecond), retries}

      failure ->
        if System.monotonic_time(:millisecond) >= deadline do
          {:error, "partition #{partition} not readable after #{@ready_deadline_ms} ms: #{reason(failure)}"}
        else
          Process.sleep(@retry_pause_ms)
          first_success(fetch, partition, deadline, retries + 1)
        end
    end
  end

  defp reason({:ok, other}), do: Runner.sanitize_error(other)
  defp reason({:error, message}), do: message

  defp stop(client) do
    GenServer.stop(client, :normal, @stop_timeout_ms)
  catch
    _, _ -> Process.exit(client, :kill)
  after
    receive do
      {:EXIT, ^client, _} -> :ok
    after
      0 -> :ok
    end
  end
end
