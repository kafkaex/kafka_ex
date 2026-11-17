defmodule Mix.Tasks.Bench.Probe do
  use Mix.Task
  alias KafkaEx.API
  alias KafkaExBench.{Build, Profile, Stats, Topics}

  @shortdoc "Per-broker produce round trip, to check a network profile"

  @warmup 20

  def run(argv) do
    {opts, _} = OptionParser.parse!(argv, strict: [profile: :string, security: :string, samples: :integer])
    profile = Profile.parse!(Keyword.get(opts, :profile, "direct"))
    security = Profile.parse_security!(Keyword.get(opts, :security, "ssl"))
    samples = Keyword.get(opts, :samples, 200)

    Mix.Task.run("app.start")
    Build.verify!()
    Profile.prepare!(profile)
    {:ok, client} = API.start_client(Profile.client_opts(profile, security))

    Topics.with_topic(client, "probe", [partitions: 3], fn topic ->
      leaders = Topics.leaders!(client, topic)
      Topics.ensure_distinct_leaders!(leaders)
      probe_partitions(client, topic, leaders, profile, security, samples)
    end)
  end

  defp probe_partitions(client, topic, leaders, profile, security, samples) do
    for {partition, node} <- Enum.sort(leaders) do
      for _ <- 1..@warmup, do: {:ok, _} = API.produce_one(client, topic, partition, "probe")

      latencies =
        for _ <- 1..samples do
          {us, {:ok, _}} = :timer.tc(fn -> API.produce_one(client, topic, partition, "probe") end)
          us
        end

      Mix.shell().info(
        "profile=#{profile} security=#{security} broker=#{node} partition=#{partition} " <>
          "p50_us=#{Stats.percentile(latencies, 50)} p99_us=#{Stats.percentile(latencies, 99)}"
      )
    end
  end
end
