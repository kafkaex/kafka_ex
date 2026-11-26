defmodule KafkaEx.Pins.MetadataContractPinTest do
  use KafkaEx.TestSupport.PinCase, async: false

  @moduletag pin_set: :invariant

  describe "produce with a nil partition" do
    test "returns :topic_not_found for a topic the broker reports unknown" do
      %{fake: fake, client: client, host: host} = start_pinned()
      FakeTransport.respond(fake, :metadata, unknown_topic_reply(host, "gone"))

      {result, _log} = with_log(fn -> API.produce(client, "gone", nil, [%{value: "v"}]) end)

      assert result == {:error, :topic_not_found}
    end

    test "returns :topic_not_found when every metadata request fails" do
      %{fake: fake, client: client} = start_pinned()
      FakeTransport.respond(fake, :metadata, :close)
      with_request_timeout(200)

      {result, _log} = with_log(fn -> API.produce(client, "uncached", nil, [%{value: "v"}]) end)

      assert result == {:error, :topic_not_found}
    end
  end

  test "a topic the broker is auto-creating is answered once it has a leader, never as unknown" do
    %{fake: fake, client: client, host: host} = start_pinned()
    replies = [leaderless_reply(host, "fresh"), metadata_reply(host, %{"fresh" => [1]})]
    counter = :counters.new(1, [])

    FakeTransport.respond(fake, :metadata, fn _request ->
      :counters.add(counter, 1, 1)
      Enum.at(replies, min(:counters.get(counter, 1), 2) - 1)
    end)

    {result, _log} = with_log(fn -> API.topics_metadata(client, ["fresh"], true) end)

    assert {:ok, [%{name: "fresh"}]} = result
  end

  test "cluster_metadata/1 carries brokers, topics, the coordinator cache and the preferred replica" do
    %{fake: fake, client: client} = start_pinned(api_versions: %{fetch: 11..11})
    FakeTransport.respond(fake, :heartbeat, %{error_code: 0})

    FakeTransport.respond(fake, :fetch, fn
      %{node_id: 1} = request -> {:frame, fetch_frame(request, 2)}
      replica -> {:frame, fetch_frame(replica, -1)}
    end)

    {:ok, :no_error} = API.heartbeat(client, "g", "m", 1)
    {:ok, _} = API.fetch(client, @topic, 0, 0, max_wait_time: 10, rack_id: "az")
    {:ok, metadata} = API.cluster_metadata(client)

    assert metadata.brokers |> Map.keys() |> Enum.sort() == [1, 2]
    assert Map.keys(metadata.topics) == [@topic]
    assert metadata.consumer_group_coordinators == %{"g" => 2}
    assert metadata.preferred_replicas == %{{@topic, 0} => 2}
  end

  test "a tracked refresh whose reply omits a topic drops it from the cache" do
    %{fake: fake, client: client, host: host} = start_pinned()
    {:ok, [_]} = API.topics_metadata(client, [@topic])
    FakeTransport.respond(fake, :metadata, metadata_reply(host, %{}))

    {{:ok, _}, _log} = with_log(fn -> refresh_tracked(client) end)
    {:ok, metadata} = API.cluster_metadata(client)

    refute Map.has_key?(metadata.topics, @topic)
  end
end
