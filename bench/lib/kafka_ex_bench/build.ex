defmodule KafkaExBench.Build do
  def kafka_ex_path do
    Path.expand(System.get_env("KAFKA_EX_PATH", ".."), Path.dirname(Mix.Project.project_file()))
  end

  def verify! do
    source = canonical(to_string(KafkaEx.API.module_info(:compile)[:source]))
    root = canonical(kafka_ex_path())

    if source_under?(source, root) do
      :ok
    else
      raise "kafka_ex was compiled from #{source}, expected a path under #{root}"
    end
  end

  def source_under?(source, root), do: String.starts_with?(canonical(source), canonical(root) <> "/")

  def canonical(path) do
    [root | parts] = path |> Path.expand() |> Path.split()
    resolve(parts, root, 0)
  end

  defp resolve([], resolved, _depth), do: resolved
  defp resolve(_parts, _resolved, 40), do: raise("too many symlink levels")

  defp resolve([part | rest], resolved, depth) do
    candidate = Path.join(resolved, part)

    case File.read_link(candidate) do
      {:ok, target} ->
        [root | parts] = target |> Path.expand(resolved) |> Path.split()
        resolve(parts ++ rest, root, depth + 1)

      {:error, _} ->
        resolve(rest, candidate, depth)
    end
  end
end
