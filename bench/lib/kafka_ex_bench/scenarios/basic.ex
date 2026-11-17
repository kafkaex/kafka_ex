defmodule KafkaExBench.Scenarios.Basic do
  @behaviour KafkaExBench.Scenario

  alias KafkaEx.API
  alias KafkaExBench.{Cluster, Job, Scenario, Topics}

  @partitions 6
  @brokers Cluster.expected_brokers()
  @replication_factor 3
  @records_per_partition 2_000
  @batch_size 10
  @payload_bytes 1024
  @fetch_max_bytes 65_536
  @fetch_max_wait_ms 100
  @acks -1
  @job_specs [
    {"produce_c1", 1},
    {"produce_c10", 10},
    {"produce_c100", 100},
    {"fetch_c1", 1},
    {"fetch_c10", 10},
    {"fetch_c100", 100}
  ]

  @impl true
  def job_specs, do: @job_specs

  @impl true
  def gates, do: Scenario.steady_gates(@job_specs, vm_reductions: true)

  @impl true
  def default_opts, do: [time: 10, warmup: 2, min_valid: 5]

  @impl true
  def setup(client, _profile) do
    {topic, layout} =
      Topics.create_with_layout!(client, "basic", partitions: @partitions, replication_factor: @replication_factor)

    payload = :crypto.strong_rand_bytes(@payload_bytes)

    Topics.cleanup_on_failure(client, topic, fn ->
      unless balanced?(layout), do: raise("unbalanced leaders: #{inspect(layout)}")
      preload!(client, topic, payload)
      %{topic: topic, payload: payload, layout: layout}
    end)
  end

  @impl true
  def params do
    %{
      partitions: @partitions,
      replication_factor: @replication_factor,
      records_per_partition: @records_per_partition,
      batch_size: @batch_size,
      payload_bytes: @payload_bytes,
      fetch_max_bytes: @fetch_max_bytes,
      fetch_max_wait_ms: @fetch_max_wait_ms,
      acks: @acks
    }
  end

  @impl true
  def layout(ctx), do: ctx.layout

  @impl true
  def jobs(client, %{topic: topic, payload: payload}) do
    Enum.map(@job_specs, fn {name, parallel} ->
      %Job{name: name, parallel: parallel, fun: job_fun(name, client, topic, payload)}
    end)
  end

  @impl true
  def check(client, %{topic: topic, layout: layout}) do
    {current, brokers} = Topics.fetch_layout!(client, topic)

    case cluster_problem(layout, current, brokers) do
      nil -> :ok
      message -> {:error, message}
    end
  end

  @impl true
  def teardown(client, %{topic: topic}) do
    Topics.delete!(client, topic)
    :ok
  end

  def preload_plan, do: %{batch_size: @batch_size, batches_per_partition: div(@records_per_partition, @batch_size)}

  def cluster_problem(layout, current, broker_count) do
    cond do
      broker_count != @brokers -> "#{broker_count} brokers, expected #{@brokers}"
      current != layout -> "leaders changed from #{inspect(layout)} to #{inspect(current)}"
      true -> nil
    end
  end

  def balanced?(layout, brokers \\ @brokers) do
    partitions = map_size(layout)
    per_broker = layout |> Map.values() |> Enum.frequencies()

    rem(partitions, brokers) == 0 and map_size(per_broker) == brokers and
      Enum.all?(per_broker, fn {_node, count} -> count == div(partitions, brokers) end)
  end

  defp job_fun("produce" <> _, client, topic, payload) do
    fn -> {:ok, _} = API.produce_one(client, topic, :rand.uniform(@partitions) - 1, payload, acks: @acks) end
  end

  defp job_fun("fetch" <> _, client, topic, _payload) do
    fn ->
      {:ok, %{records: [_ | _]}} =
        API.fetch(client, topic, :rand.uniform(@partitions) - 1, :rand.uniform(@records_per_partition) - 1,
          max_wait_time: @fetch_max_wait_ms,
          max_bytes: @fetch_max_bytes
        )
    end
  end

  defp preload!(client, topic, payload) do
    batch = for _ <- 1..@batch_size, do: %{value: payload}

    for partition <- 0..(@partitions - 1), _ <- 1..div(@records_per_partition, @batch_size) do
      {:ok, _} = API.produce(client, topic, partition, batch, acks: @acks)
    end

    :ok
  end
end
