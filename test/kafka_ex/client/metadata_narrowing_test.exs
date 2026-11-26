defmodule KafkaEx.Client.MetadataNarrowingTest do
  @moduledoc """
  Characterizes the tracked-topics-only refresh: a refresh can narrow
  cluster_metadata down to the tracked set, but the effect is bounded to one
  extra metadata round-trip on next use, never permanent data loss.
  """
  use KafkaEx.TestSupport.PinCase, async: false

  alias KafkaEx.Client.NodeSelector
  alias KafkaEx.Cluster.ClusterMetadata

  @moduletag pin_set: :invariant

  # Models "t2/t3 were hit-resolved but never tracked": the boot answer lists all three, the client tracks only t1.
  setup do
    pinned = start_pinned(topics: %{"t1" => [1], "t2" => [1], "t3" => [1]}, client: [initial_topics: ["t1"]])
    {:ok, booted} = API.cluster_metadata(pinned.client)
    assert booted.topics |> Map.keys() |> Enum.sort() == ["t1", "t2", "t3"]
    {:ok, pinned}
  end

  test "narrowing: refresh limited to tracked topics drops untracked topics from cluster_metadata", %{
    fake: fake,
    client: client,
    host: host
  } do
    FakeTransport.respond(fake, :metadata, metadata_reply(host, %{"t1" => [1]}))

    {{:ok, cluster_metadata}, _log} = with_log(fn -> GenServer.call(client, :update_metadata) end)

    assert cluster_metadata.topics["t1"]
    refute cluster_metadata.topics["t2"]
    refute cluster_metadata.topics["t3"]

    # The dropped topic forces a metadata round-trip on its next use, not a data loss.
    {:ok, snapshot} = API.cluster_metadata(client)
    assert {:error, _} = ClusterMetadata.select_node(snapshot, NodeSelector.topic_partition("t2", 0))
  end

  test "recovery: a topic_metadata call for the dropped topic re-adds it without losing the retained one", %{
    fake: fake,
    client: client,
    host: host
  } do
    FakeTransport.respond(fake, :metadata, metadata_reply(host, %{"t1" => [1]}))
    {{:ok, _}, _log} = with_log(fn -> GenServer.call(client, :update_metadata) end)

    FakeTransport.respond(fake, :metadata, metadata_reply(host, %{"t1" => [1], "t2" => [1]}))

    {{:ok, _topic_metadata}, _log} =
      with_log(fn -> Client.request(client, {:topic_metadata, ["t2"], [allow_topic_creation: false]}, 5_000) end)

    {:ok, recovered} = API.cluster_metadata(client)
    assert recovered.topics["t1"]
    assert recovered.topics["t2"]
    assert tracked_topics(fake, client) == MapSet.new(["t1", "t2"])
  end

  test "convergence: once tracked, a topic survives further refreshes (no thrashing)", %{
    fake: fake,
    client: client,
    host: host
  } do
    FakeTransport.respond(fake, :metadata, metadata_reply(host, %{"t1" => [1]}))
    {{:ok, _}, _log} = with_log(fn -> GenServer.call(client, :update_metadata) end)

    FakeTransport.respond(fake, :metadata, metadata_reply(host, %{"t1" => [1], "t2" => [1]}))

    {{:ok, _}, _log} =
      with_log(fn -> Client.request(client, {:topic_metadata, ["t2"], [allow_topic_creation: false]}, 5_000) end)

    {{:ok, cluster_metadata}, _log} = with_log(fn -> GenServer.call(client, :update_metadata) end)

    assert cluster_metadata.topics["t1"]
    assert cluster_metadata.topics["t2"]
    assert tracked_topics(fake, client) == MapSet.new(["t1", "t2"])
  end

  # A refresh asks for exactly the tracked topics.
  defp tracked_topics(fake, client) do
    with_log(fn -> GenServer.call(client, :update_metadata) end)
    fake |> requests(:metadata) |> List.last() |> requested_topics()
  end

  defp requested_topics(%{api_version: 0, frame: frame}) do
    <<_header::binary-size(8), client_id_size::16-signed, _client_id::binary-size(client_id_size), count::32-signed,
      rest::binary>> = frame

    {names, _rest} =
      Enum.map_reduce(1..count//1, rest, fn _, <<size::16-signed, name::binary-size(size), tail::binary>> ->
        {name, tail}
      end)

    MapSet.new(names)
  end
end
