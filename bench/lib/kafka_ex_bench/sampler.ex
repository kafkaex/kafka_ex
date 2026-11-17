defmodule KafkaExBench.Sampler do
  @empty %{max_mailbox: 0, first_reductions: nil, last_reductions: 0, samples: 0}

  def start(pid, interval_ms \\ 10) do
    spawn(fn -> loop(pid, interval_ms, observe(pid, @empty)) end)
  end

  def stop(sampler) do
    monitor = Process.monitor(sampler)
    ref = make_ref()
    send(sampler, {:stop, self(), ref})

    receive do
      {^ref, summary} ->
        Process.demonitor(monitor, [:flush])
        summary

      {:DOWN, ^monitor, _, _, reason} ->
        raise "sampler exited before replying: #{inspect(reason)}"
    after
      5_000 ->
        Process.demonitor(monitor, [:flush])
        raise "sampler did not reply within 5000 ms"
    end
  end

  defp loop(pid, interval_ms, acc) do
    receive do
      {:stop, from, ref} -> send(from, {ref, summary(acc)})
    after
      interval_ms ->
        case Process.alive?(pid) do
          true -> loop(pid, interval_ms, observe(pid, acc))
          false -> await_stop(acc)
        end
    end
  end

  defp await_stop(acc) do
    receive do
      {:stop, from, ref} -> send(from, {ref, summary(acc)})
    end
  end

  defp observe(pid, acc) do
    case Process.info(pid, [:message_queue_len, :reductions]) do
      [message_queue_len: mailbox, reductions: reductions] ->
        %{
          acc
          | max_mailbox: max(acc.max_mailbox, mailbox),
            first_reductions: acc.first_reductions || reductions,
            last_reductions: reductions,
            samples: acc.samples + 1
        }

      nil ->
        acc
    end
  end

  defp summary(%{first_reductions: nil}), do: %{max_mailbox: 0, reductions: 0, samples: 0}

  defp summary(acc) do
    %{
      max_mailbox: acc.max_mailbox,
      reductions: max(acc.last_reductions - acc.first_reductions, 0),
      samples: acc.samples
    }
  end
end
