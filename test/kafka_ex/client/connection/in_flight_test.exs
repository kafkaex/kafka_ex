defmodule KafkaEx.Client.Connection.InFlightTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Client.Connection.InFlight

  @max_id 2_147_483_647

  defp entry(live_until \\ 0), do: %{from: {self(), make_ref()}, live_until: live_until}

  test "ids start at the injected start" do
    assert {41, _} = InFlight.next_id(InFlight.new(41))
    assert {0, _} = InFlight.next_id(InFlight.new())
  end

  test "ids wrap from the largest int32 to 0" do
    {first, in_flight} = InFlight.next_id(InFlight.new(@max_id))
    {second, _} = InFlight.next_id(in_flight)

    assert {first, second} == {@max_id, 0}
  end

  test "a registered id is skipped" do
    in_flight = InFlight.put(InFlight.new(5), 5, entry())

    assert {6, _} = InFlight.next_id(in_flight)
  end

  test "pop of an unknown id and a second pop of the same id are :error" do
    in_flight = InFlight.put(InFlight.new(), 3, entry())

    assert InFlight.pop(in_flight, 4) == :error
    assert {:ok, _entry, in_flight} = InFlight.pop(in_flight, 3)
    assert InFlight.pop(in_flight, 3) == :error
    assert InFlight.empty?(in_flight)
  end

  test "abandon keeps the id registered without a caller" do
    in_flight = InFlight.new() |> InFlight.put(3, entry()) |> InFlight.abandon(3)

    refute InFlight.empty?(in_flight)
    assert [{3, %{from: nil}}] = InFlight.entries(in_flight)
    assert {:ok, %{from: nil}, _} = InFlight.pop(in_flight, 3)
  end

  test "earliest_live_until is the smallest bound, or nil when empty" do
    in_flight = InFlight.new() |> InFlight.put(1, entry(300)) |> InFlight.put(2, entry(100))

    assert InFlight.earliest_live_until(in_flight) == 100
    assert InFlight.earliest_live_until(InFlight.new()) == nil
  end

  test "awaited? is true only while an entry still has a caller" do
    in_flight = InFlight.new() |> InFlight.put(1, entry()) |> InFlight.put(2, entry())

    assert InFlight.awaited?(in_flight)
    assert InFlight.awaited?(InFlight.abandon(in_flight, 1))
    refute in_flight |> InFlight.abandon(1) |> InFlight.abandon(2) |> InFlight.awaited?()
    refute InFlight.awaited?(InFlight.new())
  end

  test "drain returns every entry and leaves no entry but keeps the next id" do
    {id, in_flight} = InFlight.next_id(InFlight.new(7))
    in_flight = in_flight |> InFlight.put(id, entry()) |> InFlight.abandon(id)

    assert {[{7, %{from: nil}}], drained} = InFlight.drain(in_flight)
    assert InFlight.empty?(drained)
    assert {8, _} = InFlight.next_id(drained)
  end
end
