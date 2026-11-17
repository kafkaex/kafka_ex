defmodule KafkaExBench.Compare.Sides do
  alias KafkaExBench.EnvInfo

  def default_opts(config) do
    bench_dir = Path.dirname(Mix.Project.project_file())

    repo_root =
      case System.cmd("git", ["-C", bench_dir, "rev-parse", "--show-toplevel"], stderr_to_stdout: true) do
        {root, 0} -> String.trim(root)
        {output, _} -> raise "cannot find the git repository of #{bench_dir}: #{String.trim(output)}"
      end

    [repo_root: repo_root, worktree_root: Path.join(config.out, "worktrees")]
  end

  def with_sides(config, opts, fun) do
    try do
      base = prepare!(config.base, "base", [], config, opts)
      candidate = prepare!(config.candidate, "candidate", config.candidate_env, config, opts)
      fun.(%{base: base, candidate: candidate})
    after
      cleanup(opts[:repo_root], Path.join(opts[:worktree_root], "base"))
      cleanup(opts[:repo_root], Path.join(opts[:worktree_root], "candidate"))
      File.rmdir(opts[:worktree_root])
    end
  end

  defp prepare!(spec, label, env, config, opts) do
    case prepare(spec, label, [allow_dirty: config.allow_dirty, env: env] ++ opts) do
      {:ok, side, _dir} -> side
      {:error, message} -> raise message
    end
  end

  def prepare(spec, label, opts) do
    if File.dir?(spec), do: path_side(spec, label, opts), else: worktree_side(spec, label, opts)
  end

  defp path_side(path, label, opts) do
    case EnvInfo.git_state(path) do
      {nil, _} ->
        {:error, "#{path} is not a git checkout of kafka_ex"}

      {sha, dirty} ->
        if dirty and not Keyword.fetch!(opts, :allow_dirty),
          do:
            {:error,
             "#{path} has uncommitted changes in lib, config, mix.exs or mix.lock; commit them or pass --allow-dirty"},
          else: {:ok, side(label, Path.expand(path), sha, opts), nil}
    end
  end

  defp worktree_side(rev, label, opts) do
    repo = Keyword.fetch!(opts, :repo_root)
    dir = Path.join(Keyword.fetch!(opts, :worktree_root), label)

    with {:ok, sha} <- resolve(repo, rev),
         :ok <- File.mkdir_p(Path.dirname(dir)),
         {_, 0} <- System.cmd("git", ["-C", repo, "worktree", "add", "--detach", dir, sha], stderr_to_stdout: true) do
      {:ok, side(label, dir, sha, opts), dir}
    else
      {:error, reason} when is_atom(reason) -> {:error, "cannot create #{dir}: #{:file.format_error(reason)}"}
      {:error, message} when is_binary(message) -> {:error, message}
      {output, _status} -> {:error, "git worktree add failed for #{rev}: #{String.trim(output)}"}
    end
  end

  defp resolve(repo, rev) do
    case System.cmd("git", ["-C", repo, "rev-parse", "--verify", "--quiet", rev <> "^{commit}"], stderr_to_stdout: true) do
      {sha, 0} -> {:ok, String.trim(sha)}
      _ -> {:error, "#{rev} is neither a directory nor a commit of #{repo}"}
    end
  end

  defp side(label, path, sha, opts), do: %{label: label, path: path, sha: sha, env: Keyword.fetch!(opts, :env)}

  defp cleanup(repo, dir) do
    if File.dir?(dir), do: System.cmd("git", ["-C", repo, "worktree", "remove", "--force", dir], stderr_to_stdout: true)
  rescue
    _ -> :ok
  end
end
