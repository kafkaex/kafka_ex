defmodule KafkaEx.Client.MetadataStore.Backoff do
  @moduledoc false

  alias KafkaEx.Support.Retry

  @type entry :: %{failures: non_neg_integer(), next_at: integer()}
  @type config :: %{min_interval: pos_integer(), base: pos_integer(), cap: pos_integer()}

  @spec delay(failures :: pos_integer(), base :: pos_integer(), cap :: pos_integer()) :: pos_integer()
  def delay(failures, base, cap) do
    round(Retry.backoff_delay(failures - 1, base, cap) * (0.8 + 0.4 * :rand.uniform()))
  end

  @spec next_at(entry() | nil, :ok | :bad | :failed, integer(), config()) :: entry()
  def next_at(_entry, :ok, now, config), do: %{failures: 0, next_at: now + config.min_interval}

  def next_at(entry, :failed, now, config), do: %{failures: failures(entry), next_at: now + config.min_interval}

  def next_at(entry, :bad, now, config) do
    failures = failures(entry) + 1
    %{failures: failures, next_at: now + max(config.min_interval, delay(failures, config.base, config.cap))}
  end

  defp failures(nil), do: 0
  defp failures(entry), do: entry.failures
end
