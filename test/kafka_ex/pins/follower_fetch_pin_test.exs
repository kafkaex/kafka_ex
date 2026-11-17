defmodule KafkaEx.Pins.FollowerFetchPinTest do
  use KafkaEx.TestSupport.PinCase, async: false

  @moduletag pin_set: :invariant

  @rack "az-pin"

  setup do
    start_pinned(api_versions: %{fetch: 11..11})
  end

  defp leader_names_replica(fake, replica_reply) do
    FakeTransport.respond(fake, :fetch, fn request ->
      if request.node_id == 1, do: {:frame, fetch_frame(request, 2)}, else: replica_reply.(request)
    end)
  end

  defp fetch(client), do: API.fetch(client, @topic, 0, 0, max_wait_time: 10, rack_id: @rack)

  test "a fetch goes to the replica the leader names, carrying the rack id", %{fake: fake, client: client} do
    leader_names_replica(fake, &{:frame, fetch_frame(&1, -1)})

    {:ok, _} = fetch(client)
    {:ok, _} = fetch(client)

    assert nodes(fake, :fetch) == [1, 2]
    assert fake |> requests(:fetch) |> Enum.all?(&(:binary.match(&1.frame, @rack) != :nomatch))
  end

  test "a replica's answer of -1 keeps the hint", %{fake: fake, client: client} do
    leader_names_replica(fake, &{:frame, fetch_frame(&1, -1)})

    for _ <- 1..3, do: {:ok, _} = fetch(client)

    assert nodes(fake, :fetch) == [1, 2, 2]
  end

  test "an explicit metadata refresh drops the hint", %{fake: fake, client: client} do
    leader_names_replica(fake, &{:frame, fetch_frame(&1, -1)})
    {:ok, _} = fetch(client)
    {:ok, _} = fetch(client)

    {:ok, _} = API.metadata(client)
    {:ok, _} = fetch(client)

    assert nodes(fake, :fetch) == [1, 2, 1]
  end

  test "the :update_metadata call drops the hint", %{fake: fake, client: client} do
    leader_names_replica(fake, &{:frame, fetch_frame(&1, -1)})
    {:ok, _} = fetch(client)
    {:ok, _} = fetch(client)

    {:ok, _} = GenServer.call(client, :update_metadata)
    {:ok, _} = fetch(client)

    assert nodes(fake, :fetch) == [1, 2, 1]
  end

  @tag pin_set: :target_state
  test "after the replica closes the connection, the leader answers within the same call", %{
    fake: fake,
    client: client
  } do
    named = :counters.new(1, [])

    FakeTransport.respond(fake, :fetch, fn
      %{node_id: 1} = request ->
        :counters.add(named, 1, 1)
        {:frame, fetch_frame(request, if(:counters.get(named, 1) == 1, do: 2, else: -1))}

      _replica ->
        :close
    end)

    {:ok, _} = fetch(client)

    {result, _log} = with_log(fn -> fetch(client) end)

    assert {:ok, _} = result
    assert nodes(fake, :fetch) == [1, 2, 1]
  end
end
