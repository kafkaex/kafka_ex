defmodule KafkaExBench.Scenarios.LargeFetch do
  @behaviour KafkaExBench.Scenario

  alias KafkaEx.API
  alias KafkaExBench.Scenarios.Basic
  alias KafkaExBench.{Job, Scenario, Topics, Workers}

  @replication_factor 3
  @large_partitions 3
  @large_record_bytes 102_400
  @large_batch_size 5
  @large_records_per_partition 40
  @large_fetch_workers 4
  @large_fetch_max_bytes 5_000_000
  @small_partitions 6
  @small_record_bytes 1024
  @small_batch_size 10
  @small_records_per_partition 2_000
  @small_fetch_max_bytes 65_536
  @small_fetch_max_wait_ms 100
  @acks -1
  @metrics [{:p50_us, :lower_better}, {:throughput_ops, :higher_better}]
  @min_samples 200
  @job_specs [{"small_produce_c10", 10}, {"small_fetch_c10", 10}]

  @impl true
  def job_specs, do: @job_specs

  @impl true
  def gates, do: Scenario.steady_gates(@job_specs)

  @impl true
  def default_opts, do: [time: 15, warmup: 2, min_valid: 5]

  @impl true
  def setup(client, _profile) do
    {large, large_layout} =
      Topics.create_with_layout!(client, "large-fetch",
        partitions: @large_partitions,
        replication_factor: @replication_factor
      )

    Topics.cleanup_on_failure(client, large, fn ->
      {small, small_layout} =
        Topics.create_with_layout!(client, "large-fetch-small",
          partitions: @small_partitions,
          replication_factor: @replication_factor
        )

      Topics.cleanup_on_failure(client, small, fn ->
        for layout <- [large_layout, small_layout], not Basic.balanced?(layout) do
          raise "unbalanced leaders: #{inspect(layout)}"
        end

        payload = :crypto.strong_rand_bytes(@small_record_bytes)
        preload!(client, large, @large_partitions, :crypto.strong_rand_bytes(@large_record_bytes), preload_plan().large)
        preload!(client, small, @small_partitions, payload, preload_plan().small)

        %{
          large: large,
          small: small,
          payload: payload,
          layouts: %{large: large_layout, small: small_layout}
        }
      end)
    end)
  end

  @impl true
  def params do
    %{
      large_partitions: @large_partitions,
      small_partitions: @small_partitions,
      replication_factor: @replication_factor,
      large_record_bytes: @large_record_bytes,
      large_batch_size: @large_batch_size,
      large_records_per_partition: @large_records_per_partition,
      large_fetch_workers: @large_fetch_workers,
      large_fetch_max_bytes: @large_fetch_max_bytes,
      small_record_bytes: @small_record_bytes,
      small_batch_size: @small_batch_size,
      small_records_per_partition: @small_records_per_partition,
      small_fetch_max_bytes: @small_fetch_max_bytes,
      small_fetch_max_wait_ms: @small_fetch_max_wait_ms,
      acks: @acks
    }
  end

  @impl true
  def layout(%{layouts: layouts}), do: %{small: layouts.small, large: layouts.large}

  @impl true
  def jobs(client, ctx) do
    background = %{start: fn -> start_large_fetchers(client, ctx.large) end, stop: &Workers.stop/1}

    for {name, parallel} <- @job_specs do
      %Job{
        name: name,
        parallel: parallel,
        fun: job_fun(name, client, ctx),
        min_samples: @min_samples,
        background: background,
        metrics: @metrics
      }
    end
  end

  @impl true
  def check(client, %{large: large, small: small, layouts: layouts}) do
    {large_current, brokers} = Topics.fetch_layout!(client, large)
    {small_current, _} = Topics.fetch_layout!(client, small)

    case Basic.cluster_problem(layouts, %{large: large_current, small: small_current}, brokers) do
      nil -> :ok
      message -> {:error, message}
    end
  end

  @impl true
  def teardown(client, %{large: large, small: small}) do
    Topics.delete!(client, large)
    Topics.delete!(client, small)
    :ok
  end

  def fetch_problem({:ok, %{records: records}}) do
    count = length(records)

    if count != @large_records_per_partition,
      do: "large fetch returned #{count} records, expected #{@large_records_per_partition}"
  end

  def fetch_problem(other), do: "large fetch failed: #{inspect(other)}"

  def preload_plan do
    %{
      large: %{
        batch_size: @large_batch_size,
        batches_per_partition: div(@large_records_per_partition, @large_batch_size),
        record_bytes: @large_record_bytes
      },
      small: %{
        batch_size: @small_batch_size,
        batches_per_partition: div(@small_records_per_partition, @small_batch_size),
        record_bytes: @small_record_bytes
      }
    }
  end

  defp preload!(client, topic, partitions, payload, %{batch_size: batch_size, batches_per_partition: batches}) do
    batch = for _ <- 1..batch_size, do: %{value: payload}

    for partition <- 0..(partitions - 1), _ <- 1..batches do
      {:ok, _} = API.produce(client, topic, partition, batch, acks: @acks)
    end

    :ok
  end

  defp start_large_fetchers(client, large) do
    Workers.start(@large_fetch_workers, &rem(&1, @large_partitions), fn partition ->
      result = API.fetch(client, large, partition, 0, max_bytes: @large_fetch_max_bytes)

      case fetch_problem(result) do
        nil -> {:ok, partition}
        message -> {:error, message}
      end
    end)
  end

  defp job_fun("small_produce_c10", client, %{small: topic, payload: payload}) do
    fn -> {:ok, _} = API.produce_one(client, topic, :rand.uniform(@small_partitions) - 1, payload, acks: @acks) end
  end

  defp job_fun("small_fetch_c10", client, %{small: topic}) do
    fn ->
      {:ok, %{records: [_ | _]}} =
        API.fetch(client, topic, :rand.uniform(@small_partitions) - 1, :rand.uniform(@small_records_per_partition) - 1,
          max_wait_time: @small_fetch_max_wait_ms,
          max_bytes: @small_fetch_max_bytes
        )
    end
  end
end
