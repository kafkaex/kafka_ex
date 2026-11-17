defmodule KafkaExBench.Workers do
  alias KafkaExBench.Runner

  def start(count, init, step) do
    owner = self()
    ref = make_ref()
    steps = :counters.new(1, [:write_concurrency])

    pids =
      for index <- 0..(count - 1) do
        spawn(fn -> run(owner, ref, steps, init.(index), step) end)
      end

    %{ref: ref, pids: pids, steps: steps}
  end

  def stop(%{ref: ref, pids: pids, steps: steps}) do
    Enum.each(pids, &Process.exit(&1, :kill))
    ops = :counters.get(steps, 1)

    case first_error(ref, nil) do
      nil -> {:ok, ops}
      message -> {:error, message, ops}
    end
  end

  defp run(owner, ref, steps, state, step) do
    case step_once(state, step) do
      {:ok, next} ->
        :counters.add(steps, 1, 1)
        run(owner, ref, steps, next, step)

      {:error, message} ->
        send(owner, {ref, :worker_error, message})
    end
  end

  defp step_once(state, step) do
    case step.(state) do
      {:ok, next} -> {:ok, next}
      {:error, message} when is_binary(message) -> {:error, message}
      other -> {:error, "unexpected worker result: " <> Runner.sanitize_error(other)}
    end
  rescue
    error -> {:error, Runner.sanitize_error(error)}
  catch
    kind, reason -> {:error, Runner.sanitize_error({:caught, kind, reason})}
  end

  defp first_error(ref, found) do
    receive do
      {^ref, :worker_error, message} -> first_error(ref, found || message)
    after
      0 -> found
    end
  end
end
