defmodule KafkaEx.Client.ConnectionStateMachine.InFlight do
  @moduledoc """
  A connection's table of requests already written and awaiting a reply, keyed by the Kafka
  correlation id: several requests can be in flight on one TCP connection, and each reply carries
  the id it answers.

  An abandoned entry (its caller timed out) stays, so a late reply is consumed silently instead of
  looking like a stream mismatch; the table is drained to answer every waiter when the socket
  drops. Ids are INT32, so the counter wraps from 2_147_483_647 to 0 and skips ids still in flight.
  """

  @max_id 2_147_483_647

  defstruct next: 0, entries: %{}

  @type t :: %__MODULE__{next: non_neg_integer(), entries: %{non_neg_integer() => map()}}

  @spec new(non_neg_integer()) :: t()
  def new(start) when is_integer(start) and start >= 0 and start <= @max_id, do: %__MODULE__{next: start}

  @spec next_id(t()) :: {non_neg_integer(), t()}
  def next_id(%__MODULE__{next: id, entries: entries} = in_flight) do
    in_flight = %{in_flight | next: advance(id)}
    if Map.has_key?(entries, id), do: next_id(in_flight), else: {id, in_flight}
  end

  defp advance(@max_id), do: 0
  defp advance(id), do: id + 1

  @spec put(t(), non_neg_integer(), map()) :: t()
  def put(in_flight, id, entry), do: %{in_flight | entries: Map.put(in_flight.entries, id, entry)}

  @spec pop(t(), integer()) :: {:ok, map(), t()} | :error
  def pop(in_flight, id) do
    case Map.pop(in_flight.entries, id) do
      {nil, _} -> :error
      {entry, entries} -> {:ok, entry, %{in_flight | entries: entries}}
    end
  end

  @spec abandon(t(), non_neg_integer()) :: t()
  def abandon(in_flight, id), do: %{in_flight | entries: Map.update!(in_flight.entries, id, &%{&1 | from: nil})}

  @spec entries(t()) :: [{non_neg_integer(), map()}]
  def entries(in_flight), do: Map.to_list(in_flight.entries)

  @spec empty?(t()) :: boolean()
  def empty?(in_flight), do: in_flight.entries == %{}

  @spec awaited?(t()) :: boolean()
  def awaited?(in_flight), do: Enum.any?(in_flight.entries, fn {_id, entry} -> entry.from != nil end)

  @spec drain(t()) :: {[{non_neg_integer(), map()}], t()}
  def drain(in_flight), do: {entries(in_flight), %{in_flight | entries: %{}}}

  @spec earliest_live_until(t()) :: integer() | nil
  def earliest_live_until(%__MODULE__{entries: entries}) when entries == %{}, do: nil

  def earliest_live_until(%__MODULE__{entries: entries}) do
    entries |> Map.values() |> Enum.map(& &1.live_until) |> Enum.min()
  end
end
