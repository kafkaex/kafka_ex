defmodule KafkaExBench.EnvInfo do
  alias KafkaExBench.{Build, Profile, Toxiproxy}

  @dep_apps [:kayrock, :telemetry, :jason, :benchee]
  @dirty_args ["status", "--porcelain", "--untracked-files=all", "--", "lib", "config", "mix.exs", "mix.lock"]

  def collect(profile, opts) do
    {sha, dirty} = git_state(Build.kafka_ex_path())

    %{
      kafka_ex_sha: sha,
      kafka_ex_dirty: dirty,
      harness_sha: harness_sha(),
      harness_dirty: harness_dirty(Path.dirname(Mix.Project.project_file())),
      kafka_ex_vsn: app_version(:kafka_ex),
      deps: Map.new(@dep_apps, &{&1, app_version(&1)}),
      otp: to_string(:erlang.system_info(:otp_release)),
      erts: to_string(:erlang.system_info(:version)),
      elixir: System.version(),
      schedulers_online: :erlang.system_info(:schedulers_online),
      scheduler_bind_type: :erlang.system_info(:scheduler_bind_type),
      logical_processors: :erlang.system_info(:logical_processors),
      arch: to_string(:erlang.system_info(:system_architecture)),
      os: os(),
      profile: profile,
      latency_ms: latency(profile, opts),
      toxiproxy_version: toxiproxy(profile, opts)
    }
  end

  defp harness_sha do
    case git(Path.dirname(Mix.Project.project_file()), ["rev-parse", "HEAD"]) do
      {sha, 0} -> String.trim(sha)
      _ -> nil
    end
  end

  def harness_dirty(bench_dir) do
    with {prefix, 0} <- git(bench_dir, ["rev-parse", "--show-prefix"]),
         {status, 0} <- git(bench_dir, ["status", "--porcelain", "--untracked-files=all", "--", "."]) do
      prefix = String.trim(prefix)
      status |> String.split("\n", trim: true) |> Enum.any?(&harness_change?(&1, prefix))
    else
      _ -> nil
    end
  end

  defp harness_change?(line, prefix) do
    {state, path} = String.split_at(line, 3)
    relative = String.replace_prefix(path, prefix, "")

    not (String.starts_with?(relative, ["_build", "deps/"]) or
           (state == "?? " and String.starts_with?(relative, "results/tmp/")))
  end

  def app_version(app) do
    case Application.spec(app, :vsn) do
      nil -> nil
      vsn -> to_string(vsn)
    end
  end

  defp os do
    {family, name} = :os.type()
    version = :os.version() |> Tuple.to_list() |> Enum.join(".")
    "#{family} #{name} #{version}"
  end

  defp latency(:latency, opts), do: Keyword.get_lazy(opts, :latency_ms, &Profile.latency_ms/0)
  defp latency(_profile, _opts), do: nil

  defp toxiproxy(:latency, opts), do: Keyword.get_lazy(opts, :toxiproxy_version, &Toxiproxy.version!/0)
  defp toxiproxy(_profile, _opts), do: nil

  def git_state(path) do
    with {sha, 0} <- git(path, ["rev-parse", "HEAD"]),
         {status, 0} <- git(path, @dirty_args) do
      {String.trim(sha), String.trim(status) != ""}
    else
      _ -> {nil, nil}
    end
  end

  defp git(path, args) do
    System.cmd("git", ["-C", path | args], stderr_to_stdout: true)
  rescue
    _ -> :error
  end
end
