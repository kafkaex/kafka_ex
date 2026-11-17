defmodule KafkaEx.Pins.ContractSurfacePinTest do
  use KafkaEx.TestSupport.PinCase, async: false

  alias KafkaEx.Cluster.ClusterMetadata
  alias KafkaEx.Config
  alias KafkaEx.Messages.ApiVersions
  alias KafkaEx.Messages.Offset
  alias KafkaEx.Messages.RecordMetadata
  alias KafkaEx.Telemetry

  @moduletag pin_set: :invariant

  defmodule PinServer do
    alias KafkaEx.Client
    alias KafkaEx.TestSupport.PinGate

    def start_link(args, name), do: Client.start_link([{:transport, PinGate} | args], name)
  end

  defp leader(%ClusterMetadata{topics: topics}, topic, partition), do: topics[topic].partition_leaders[partition]

  describe "start_link/2" do
    test "returns a client that is registered under its name and ready to serve" do
      name = :"pin_client_#{System.unique_integer([:positive])}"
      %{fake: fake, client: client} = start_pinned(name: name)
      FakeTransport.respond(fake, :produce, produce_reply())

      assert Process.whereis(name) == client
      assert {:ok, metadata} = API.cluster_metadata(name)
      assert metadata.brokers |> Map.keys() |> Enum.sort() == [1, 2]
      assert leader(metadata, @topic, 0) == 1
      assert {:ok, %RecordMetadata{base_offset: 7}} = API.produce(name, @topic, 0, [%{value: "v"}])
    end

    test "fails fast when the cluster cannot be bootstrapped" do
      Process.flag(:trap_exit, true)
      host = "pin-#{System.unique_integer([:positive])}"
      fake = start_supervised!({FakeTransport, brokers: [{1, host, 9092}]})
      FakeTransport.respond(fake, :api_versions, :close)
      opts = [uris: [{host, 9092}], consumer_group: :no_consumer_group, transport: FakeTransport]

      {result, _log} =
        with_log(fn -> Client.start_link([{:ssl_options, FakeTransport.ssl_options(fake)} | opts], :no_name) end)

      assert {:error, _reason} = result
    end
  end

  describe "explicit metadata calls refresh the cache and return the new snapshot" do
    setup do
      start_pinned()
    end

    test "metadata/2", %{fake: fake, client: client, host: host} do
      FakeTransport.respond(fake, :metadata, metadata_reply(host, %{@topic => [2, 1]}))

      assert {:ok, returned} = API.metadata(client)
      assert {:ok, cached} = API.cluster_metadata(client)
      assert leader(returned, @topic, 0) == 2
      assert leader(cached, @topic, 0) == 2
    end

    test "metadata/3 with topics", %{fake: fake, client: client, host: host} do
      FakeTransport.respond(fake, :metadata, metadata_reply(host, %{@topic => [2, 1]}))

      assert {:ok, returned} = API.metadata(client, [@topic], [])
      assert {:ok, cached} = API.cluster_metadata(client)
      assert leader(returned, @topic, 0) == 2
      assert leader(cached, @topic, 0) == 2
    end

    test "the :update_metadata call", %{fake: fake, client: client, host: host} do
      FakeTransport.respond(fake, :metadata, metadata_reply(host, %{@topic => [2, 1]}))

      assert {:ok, returned} = GenServer.call(client, :update_metadata)
      assert {:ok, cached} = API.cluster_metadata(client)
      assert leader(returned, @topic, 0) == 2
      assert leader(cached, @topic, 0) == 2
    end
  end

  test "topics_metadata/3 sends its allow_auto_topic_creation once and does not leak it into later refreshes" do
    %{fake: fake, client: client} =
      start_pinned(api_versions: %{metadata: 0..4}, client: [allow_auto_topic_creation: false])

    booted = count(fake, :metadata)

    assert {:ok, [_topic]} = API.topics_metadata(client, [@topic], true)
    assert {:ok, _} = GenServer.call(client, :update_metadata)

    flags = fake |> requests(:metadata) |> Enum.drop(booted) |> Enum.map(&:binary.last(&1.frame))
    assert flags == [1, 0]
  end

  test "api_versions/2 is a live query, not the versions negotiated at boot" do
    %{fake: fake, client: client} = start_pinned()

    FakeTransport.respond(fake, :api_versions, %{
      api_keys: [%{api_key: 18, min_version: 0, max_version: 0}, %{api_key: 3, min_version: 0, max_version: 1}]
    })

    assert {:ok, %ApiVersions{api_versions: versions}} = API.api_versions(client)
    assert versions == %{3 => %{min_version: 0, max_version: 1}, 18 => %{min_version: 0, max_version: 0}}
  end

  test "an ordinary refresh failure keeps the client up on its last snapshot" do
    %{fake: fake, client: client} = start_pinned(client: [metadata_update_interval: 50])
    {:ok, before} = API.cluster_metadata(client)
    FakeTransport.subscribe(fake)
    FakeTransport.respond(fake, :metadata, :close)

    capture_log(fn ->
      assert_receive {:fake_transport, :request, %{api: :metadata, action: :close}}, 5_000
      assert_receive {:fake_transport, :request, %{api: :metadata, action: :close}}, 5_000
    end)

    assert Process.alive?(client)
    assert {:ok, now} = API.cluster_metadata(client)
    assert now.topics == before.topics
    assert Map.keys(now.brokers) == Map.keys(before.brokers)
  end

  describe "the legacy worker API" do
    test "consumer_group/1 and set_consumer_group_for_auto_commit/2" do
      %{client: client} = start_pinned()

      assert KafkaEx.consumer_group(client) == :no_consumer_group
      assert :ok = API.set_consumer_group_for_auto_commit(client, "pin-group")
      assert KafkaEx.consumer_group(client) == "pin-group"
    end

    test "the {:offset, topic, partition, timestamp} call" do
      %{fake: fake, client: client} = start_pinned()

      FakeTransport.respond(fake, :list_offsets, %{
        responses: [%{topic: @topic, partition_responses: [%{partition: 0, timestamp: -1, offset: 42}]}]
      })

      assert {:ok, [%Offset{topic: @topic, partition_offsets: [partition_offset]}]} =
               GenServer.call(client, {:offset, @topic, 0, -1})

      assert partition_offset.offset == 42
    end

    test "start_link_worker/2 starts a named worker with the given consumer group" do
      host = "pin-#{System.unique_integer([:positive])}"
      fake = start_supervised!({FakeTransport, brokers: [{1, host, 9092}]})
      name = :"pin_worker_#{System.unique_integer([:positive])}"

      opts = [
        uris: [{host, 9092}],
        consumer_group: "pin-group",
        use_ssl: false,
        ssl_options: PinGate.ssl_options(PinGate.new(), fake),
        server_impl: PinServer
      ]

      assert {:ok, worker} = KafkaEx.start_link_worker(name, opts)
      on_exit(fn -> if Process.alive?(worker), do: GenServer.stop(worker) end)
      assert Process.whereis(name) == worker
      assert KafkaEx.consumer_group(name) == "pin-group"
    end

    test "disable_default_worker leaves no default worker running" do
      assert Config.disable_default_worker()
      assert Process.whereis(Config.default_worker()) == nil
    end
  end

  test "telemetry event names and measurement keys" do
    %{fake: fake, client: client, host: host} = start_pinned()
    FakeTransport.respond(fake, :produce, produce_reply())
    FakeTransport.respond(fake, :fetch, &{:frame, fetch_frame(&1, -1)})
    test = self()
    handler = "pin-telemetry-#{host}"

    :telemetry.attach_many(
      handler,
      Telemetry.events(),
      fn event, measurements, _metadata, _config ->
        send(test, {:pin_event, event, measurements |> Map.keys() |> Enum.sort()})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)
    watch_close(host)

    {:ok, _} = API.produce(client, @topic, 0, [%{value: "v"}])
    {:ok, _} = API.fetch(client, @topic, 0, 0, max_wait_time: 10)
    {:ok, _} = API.metadata(client)
    FakeTransport.close(fake, {host, 9092})
    assert_receive {:pin_closed, %{port: 9092}}, 5_000
    {:ok, _} = API.produce(client, @topic, 0, [%{value: "v"}])

    expected = %{
      [:kafka_ex, :produce, :start] => [:monotonic_time, :system_time],
      [:kafka_ex, :produce, :stop] => [:duration, :monotonic_time],
      [:kafka_ex, :fetch, :start] => [:monotonic_time, :system_time],
      [:kafka_ex, :fetch, :stop] => [:duration, :monotonic_time],
      [:kafka_ex, :request, :start] => [:monotonic_time, :system_time],
      [:kafka_ex, :request, :stop] => [:duration, :monotonic_time],
      [:kafka_ex, :metadata, :update, :start] => [:monotonic_time, :system_time],
      [:kafka_ex, :metadata, :update, :stop] => [:duration, :monotonic_time],
      [:kafka_ex, :connection, :start] => [:monotonic_time, :system_time],
      [:kafka_ex, :connection, :stop] => [:duration, :monotonic_time],
      [:kafka_ex, :connection, :close] => [:count]
    }

    seen = collect_events(%{})
    assert Map.take(seen, Map.keys(expected)) == expected
  end

  defp collect_events(seen) do
    receive do
      {:pin_event, event, keys} -> collect_events(Map.put(seen, event, keys))
    after
      0 -> seen
    end
  end
end
