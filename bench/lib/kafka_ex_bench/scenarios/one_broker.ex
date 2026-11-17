defmodule KafkaExBench.Scenarios.OneBroker do
  @behaviour KafkaExBench.Scenario

  alias KafkaEx.API
  alias KafkaExBench.{Cluster, Job, Scenario, Topics}

  @partitions 6
  @brokers Cluster.expected_brokers()
  @replication_factor 3
  @leader_broker 1
  @payload_bytes 1024
  @acks -1
  @job_specs [{"produce_spread_c100", 100}, {"produce_single_partition_c100", 100}]

  @impl true
  def job_specs, do: @job_specs

  @impl true
  def gates, do: Scenario.steady_gates(@job_specs, vm_reductions: true)

  @impl true
  def default_opts, do: [time: 10, warmup: 2, min_valid: 5]

  @impl true
  def setup(client, _profile) do
    {topic, layout} =
      Topics.create_with_layout!(client, "one-broker",
        partitions: @partitions,
        replica_assignment: replica_assignment(@partitions, @brokers)
      )

    Topics.cleanup_on_failure(client, topic, fn ->
      if problem = cluster_problem(layout, @brokers), do: raise(problem)
      %{topic: topic, payload: :crypto.strong_rand_bytes(@payload_bytes), layout: layout}
    end)
  end

  @impl true
  def params do
    %{
      partitions: @partitions,
      replication_factor: @replication_factor,
      leader_broker: @leader_broker,
      payload_bytes: @payload_bytes,
      acks: @acks
    }
  end

  @impl true
  def layout(ctx), do: ctx.layout

  @impl true
  def jobs(client, %{topic: topic, payload: payload}) do
    for {name, parallel} <- @job_specs do
      %Job{name: name, parallel: parallel, fun: job_fun(name, client, topic, payload)}
    end
  end

  @impl true
  def check(client, %{topic: topic}) do
    {current, brokers} = Topics.fetch_layout!(client, topic)

    case cluster_problem(current, brokers) do
      nil -> :ok
      message -> {:error, message}
    end
  end

  @impl true
  def teardown(client, %{topic: topic}) do
    Topics.delete!(client, topic)
    :ok
  end

  def replica_assignment(partitions, brokers) do
    followers = Enum.to_list(2..brokers//1)

    for partition <- 0..(partitions - 1) do
      ordered = if rem(partition, 2) == 0, do: followers, else: Enum.reverse(followers)
      {partition, [@leader_broker | ordered]}
    end
  end

  def cluster_problem(layout, broker_count) do
    cond do
      broker_count != @brokers ->
        "#{broker_count} brokers, expected #{@brokers}"

      Enum.any?(layout, fn {_partition, leader} -> leader != @leader_broker end) ->
        "not all leaders on broker #{@leader_broker}: #{inspect(layout)}"

      true ->
        nil
    end
  end

  defp job_fun("produce_spread_c100", client, topic, payload) do
    fn -> {:ok, _} = API.produce_one(client, topic, :rand.uniform(@partitions) - 1, payload, acks: @acks) end
  end

  defp job_fun("produce_single_partition_c100", client, topic, payload) do
    fn -> {:ok, _} = API.produce_one(client, topic, 0, payload, acks: @acks) end
  end
end
