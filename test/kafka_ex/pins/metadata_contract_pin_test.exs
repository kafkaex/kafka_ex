defmodule KafkaEx.Pins.MetadataContractPinTest do
  use KafkaEx.TestSupport.PinCase, async: false

  @moduletag pin_set: :invariant

  defmodule CountPartitioner do
    @behaviour KafkaEx.Producer.Partitioner

    def assign_partition(_topic, _key, _value, partition_count) do
      send(:count_partitioner_test, {:partition_count, partition_count})
      0
    end
  end

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

    test "takes the partition count from the client's metadata, sending no Metadata request" do
      %{fake: fake, client: client} = start_pinned(topics: %{@topic => [1, 1, 2]})
      FakeTransport.respond(fake, :produce, produce_reply())
      sent = count(fake, :metadata)

      for n <- 1..20, do: {:ok, _} = API.produce(client, @topic, nil, [%{key: "k#{n}", value: "v"}])

      assert count(fake, :metadata) == sent
    end

    test "uses partitions added by the broker after the tracked refresh" do
      %{fake: fake, client: client, host: host} = start_pinned(topics: %{@topic => [1, 1]})
      FakeTransport.respond(fake, :produce, produce_reply())
      Process.register(self(), :count_partitioner_test)
      opts = [partitioner: __MODULE__.CountPartitioner]
      {:ok, _} = API.produce(client, @topic, nil, [%{key: "k", value: "v"}], opts)
      assert_received {:partition_count, 2}

      FakeTransport.respond(fake, :metadata, metadata_reply(host, %{@topic => [1, 1, 2, 2]}))
      {:ok, _} = refresh_tracked(client)
      {:ok, _} = API.produce(client, @topic, nil, [%{key: "k", value: "v"}], opts)

      assert_received {:partition_count, 4}
    end

    test "topics_metadata/2 asks the broker for a topic already in the table" do
      %{fake: fake, client: client} = start_pinned()
      sent = count(fake, :metadata)

      {:ok, [_]} = API.topics_metadata(client, [@topic])

      assert count(fake, :metadata) > sent
    end

    test "refreshes once for a topic the client has no metadata for, then tracks it" do
      %{fake: fake, client: client, host: host} = start_pinned()
      FakeTransport.respond(fake, :produce, produce_reply("fresh"))
      FakeTransport.respond(fake, :metadata, metadata_reply(host, %{@topic => [1, 2], "fresh" => [1, 2]}))
      sent = count(fake, :metadata)

      {:ok, _} = API.produce(client, "fresh", nil, [%{key: "k", value: "v"}])
      {:ok, _} = API.produce(client, "fresh", nil, [%{key: "k", value: "v"}])
      refreshed = count(fake, :metadata)
      {:ok, _} = refresh_tracked(client)

      assert refreshed == sent + 1
      assert fake |> requests(:metadata) |> List.last() |> metadata_topics() == ["fresh"]
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
