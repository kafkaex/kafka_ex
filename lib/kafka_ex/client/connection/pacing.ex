defmodule KafkaEx.Client.Connection.Pacing do
  @moduledoc false

  import Bitwise, only: [<<<: 2]

  @base_ms 100
  @cap_ms 500

  @spec new() :: :ets.tid()
  def new, do: :ets.new(__MODULE__, [:set, :public, read_concurrency: true, write_concurrency: true])

  @spec claim(:ets.tid(), {binary(), :inet.port_number()}, integer()) :: :ok | {:wait, pos_integer()}
  def claim(pacing, key, now) do
    :ets.insert_new(pacing, {key, now, 0})
    [{^key, next_at, failures}] = :ets.lookup(pacing, key)

    if next_at > now do
      {:wait, next_at - now}
    else
      bumped = {key, now + delay(failures + 1), failures + 1}
      swap = [{{key, next_at, failures}, [], [{:const, bumped}]}]
      if :ets.select_replace(pacing, swap) == 1, do: :ok, else: claim(pacing, key, now)
    end
  end

  @spec connected(:ets.tid(), term(), integer()) :: true
  def connected(pacing, key, now) do
    reopened = :ets.update_element(pacing, key, {2, now}) or :ets.insert(pacing, {key, now, 0})

    for [pid] <- :ets.match(pacing, {{:first_connect, key, :"$1"}}) do
      :ets.delete(pacing, {:first_connect, key, pid})
      send(pid, :pacing_slot)
    end

    reopened
  end

  # A connection that has not attempted yet waits on another's reservation only until that one connects.
  @spec await_slot(:ets.tid(), term(), pid()) :: true
  def await_slot(pacing, key, pid), do: :ets.insert(pacing, {{:first_connect, key, pid}})

  @spec stop_awaiting_slot(:ets.tid(), term(), pid()) :: true
  def stop_awaiting_slot(pacing, key, pid), do: :ets.delete(pacing, {:first_connect, key, pid})

  @spec stable(:ets.tid(), term()) :: true
  def stable(pacing, key), do: :ets.update_element(pacing, key, {3, 0}) or :ets.insert(pacing, {key, 0, 0})

  @spec delay(pos_integer()) :: pos_integer()
  def delay(failures) when is_integer(failures) and failures > 0 do
    raw = (@base_ms <<< min(failures - 1, 16)) * uniform(0.75, 1.5)
    if raw < @cap_ms, do: round(raw), else: round(@cap_ms * uniform(0.8, 1.0))
  end

  defp uniform(low, high), do: low + (high - low) * :rand.uniform()
end
