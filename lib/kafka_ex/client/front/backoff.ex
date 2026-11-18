defmodule KafkaEx.Client.Front.Backoff do
  @moduledoc false

  alias KafkaEx.Support.Retry

  @type config :: %{base: pos_integer(), cap: pos_integer(), jitter: float()}

  @default %{base: 100, cap: 1_000, jitter: 0.2}

  @spec default() :: config()
  def default, do: @default

  @spec delay(non_neg_integer(), float(), config()) :: pos_integer()
  def delay(spent, u, %{jitter: jitter, cap: cap} = config) when is_float(u) and u >= 0.0 and u < 1.0 do
    nominal = nominal(spent, config)
    min(round(nominal * (1 - jitter + 2 * jitter * u)), cap)
  end

  @spec floor(non_neg_integer(), config()) :: non_neg_integer()
  def floor(spent, %{jitter: jitter} = config), do: trunc(nominal(spent, config) * (1 - jitter))

  @spec wake_at(integer(), non_neg_integer(), float(), integer(), config()) :: {:ok, integer()} | :no_time
  def wake_at(now, spent, u, deadline, config) do
    at = now + delay(spent, u, config)
    if at < deadline, do: {:ok, at}, else: :no_time
  end

  defp nominal(spent, %{base: base, cap: cap}), do: Retry.backoff_delay(max(spent - 1, 0), base, cap)
end
