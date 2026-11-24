defmodule KafkaEx.Client.ConnectionStateMachine.ReconnectBackoff do
  @moduledoc """
  One table per client keyed by broker address `{host, port}`, so every connection to a broker,
  whatever its role, shares one reconnect backoff, like librdkafka's and Java's `reconnect.backoff.ms`.

  `claim/3` grants one attempt per interval, atomically: one winner among concurrent callers. The
  interval starts at 100 ms and doubles with jitter, capped at about 500 ms. A successful connect
  reopens the gate and wakes connections that have not attempted yet (`await_slot/3`); a connection
  stable for `stable_period` resets the failure count.
  """

  alias KafkaEx.Support.Retry

  @base_ms 100
  @cap_ms 500

  @spec new() :: :ets.tid()
  def new, do: :ets.new(__MODULE__, [:set, :public, read_concurrency: true, write_concurrency: true])

  @spec claim(:ets.tid(), {binary(), :inet.port_number()}, integer()) :: :ok | {:wait, pos_integer()}
  def claim(table, key, now) do
    :ets.insert_new(table, {key, now, 0})
    [{^key, next_at, failures}] = :ets.lookup(table, key)

    if next_at > now do
      {:wait, next_at - now}
    else
      bumped = {key, now + delay(failures + 1), failures + 1}
      swap = [{{key, next_at, failures}, [], [{:const, bumped}]}]
      if :ets.select_replace(table, swap) == 1, do: :ok, else: claim(table, key, now)
    end
  end

  @spec connected(:ets.tid(), term(), integer()) :: boolean()
  def connected(table, key, now) do
    reopened = :ets.update_element(table, key, {2, now})

    for [pid] <- :ets.match(table, {{:first_connect, key, :"$1"}}) do
      :ets.delete(table, {:first_connect, key, pid})
      send(pid, :reconnect_slot)
    end

    reopened
  end

  # A connection that has not attempted yet waits on another's reservation only until that one connects.
  @spec await_slot(:ets.tid(), term(), pid()) :: true
  def await_slot(table, key, pid), do: :ets.insert(table, {{:first_connect, key, pid}})

  @spec stop_awaiting_slot(:ets.tid(), term(), pid()) :: true
  def stop_awaiting_slot(table, key, pid), do: :ets.delete(table, {:first_connect, key, pid})

  @spec stable(:ets.tid(), term()) :: boolean()
  def stable(table, key), do: :ets.update_element(table, key, {3, 0})

  defp delay(failures) when is_integer(failures) and failures > 0 do
    Retry.backoff_delay_jittered(failures - 1, @base_ms, @cap_ms)
  end
end
