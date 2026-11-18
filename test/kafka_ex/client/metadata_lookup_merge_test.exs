defmodule KafkaEx.Client.MetadataLookupMergeTest do
  @moduledoc """
  A targeted `metadata/3` lookup (by name or by id) lists only the requested
  topics. It must return exactly that response, but merge it into the client's
  cached metadata rather than replace it: replacing dropped every other known
  topic, the cached group coordinators, and the open broker sockets (which were
  then leaked, as nothing referenced them any more).

  Driven through a started client on the fake transport.
  """
  use KafkaEx.TestSupport.PinCase, async: false

  alias KafkaEx.Cluster.ClusterMetadata

  @moduletag pin_set: :invariant

  @offset_fetch_ok %{
    error_code: 0,
    topics: [%{name: "a", partitions: [%{partition_index: 0, committed_offset: 0, metadata: nil, error_code: 0}]}]
  }

  setup do
    %{fake: fake, client: client, host: host} =
      pinned = start_pinned(topics: %{"a" => [1]}, api_versions: %{offset_fetch: 0..3})

    FakeTransport.respond(fake, :offset_fetch, @offset_fetch_ok)
    {:ok, _} = offset_fetch(client)
    FakeTransport.respond(fake, :metadata, metadata_reply(host, %{"b" => [1]}))
    {:ok, pinned}
  end

  test "a by-name lookup returns only the requested topic", %{client: client} do
    {:ok, %ClusterMetadata{topics: topics}} = GenServer.call(client, {:metadata, ["b"], [topics: ["b"]], 0})

    assert Map.keys(topics) == ["b"]
  end

  test "...but keeps the other cached topics, the coordinators and the open socket", %{
    fake: fake,
    client: client,
    host: host
  } do
    connects = length(FakeTransport.connects(fake))
    watch_close(host)

    {:ok, _} = GenServer.call(client, {:metadata, ["b"], [topics: ["b"]], 0})

    {:ok, cached} = API.cluster_metadata(client)
    assert cached.topics |> Map.keys() |> Enum.sort() == ["a", "b"]
    assert_coordinator_cached(fake, client)
    # Uses node 1 too, so a lost node 1 socket shows up even if reconnects become lazy.
    FakeTransport.respond(fake, :produce, produce_reply("a"))
    {:ok, _} = GenServer.call(client, {:produce, "a", 0, [%{value: "v"}], []})
    assert nodes(fake, :produce) == [1]
    # Before the fix the broker came back with `socket: nil` and the old socket leaked.
    assert length(FakeTransport.connects(fake)) == connects
    refute_received {:pin_closed, _}
  end

  test "a full lookup (no topics filter) replaces the topic map", %{fake: fake, client: client} do
    {:ok, _} = GenServer.call(client, {:metadata, nil, [], 0})

    # The response listed every topic ("b" only), so "a" is gone, as on a refresh.
    {:ok, cached} = API.cluster_metadata(client)
    assert Map.keys(cached.topics) == ["b"]
    assert_coordinator_cached(fake, client)
  end

  test "the full-lookup check is version-aware: V0 sends [] for all topics, V1+ sends null" do
    # V0's topic array is not nullable, so "all topics" is []; from V1 it is null.
    v0_all = KafkaEx.Protocol.KayrockProtocol.build_request(:metadata, 0, topics: nil, topic_ids: [])
    v1_all = KafkaEx.Protocol.KayrockProtocol.build_request(:metadata, 1, topics: nil, topic_ids: [])
    v1_named = KafkaEx.Protocol.KayrockProtocol.build_request(:metadata, 1, topics: ["b"], topic_ids: [])

    assert v0_all.topics == []
    assert v1_all.topics == nil
    assert [%{name: "b"}] = v1_named.topics
  end

  defp offset_fetch(client), do: GenServer.call(client, {:offset_fetch, "g", [{"a", [%{partition_num: 0}]}], []})

  # A cached coordinator serves the next group request without a FindCoordinator.
  defp assert_coordinator_cached(fake, client) do
    found = count(fake, :find_coordinator)
    {:ok, _} = offset_fetch(client)
    assert count(fake, :find_coordinator) == found
    assert fake |> nodes(:offset_fetch) |> List.last() == 2
  end
end
