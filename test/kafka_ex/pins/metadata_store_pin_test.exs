defmodule KafkaEx.Pins.MetadataStorePinTest do
  use KafkaEx.TestSupport.PinCase, async: false

  alias KafkaEx.Client.MetadataStore
  alias KafkaEx.Client.ResponseParser
  alias KafkaEx.Client.Supervisor, as: ClientSupervisor
  alias KafkaEx.TestSupport.StoreCase
  alias KafkaEx.TestSupport.FakeTransport.Wire

  @moduletag pin_set: :invariant

  test "a raising periodic refresh degrades the store, keeps the client up, and recovers" do
    %{fake: fake, client: client, host: host} =
      start_pinned(api_versions: %{metadata: 12..12}, client: [metadata_update_interval: 100])

    test = self()
    handler = "pin-degraded-#{host}"

    :telemetry.attach_many(
      handler,
      [[:kafka_ex, :metadata_store, :degraded], [:kafka_ex, :metadata_store, :recovered]],
      fn event, measurements, metadata, _ ->
        send(test, {:pin_store, event, Map.keys(measurements), Map.keys(metadata)})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)

    degraded_log =
      capture_log(fn ->
        FakeTransport.respond(fake, :metadata, &{:frame, null_topics_metadata_frame(&1)})
        assert_receive {:pin_store, [:kafka_ex, :metadata_store, :degraded], [:system_time], [:reason]}, 5_000
      end)

    assert degraded_log =~ ~r/\[warning\].*Periodic metadata update failed: /

    assert Process.alive?(client)
    assert {:ok, %{topics: %{@topic => _}}} = API.cluster_metadata(client)

    recovered_log =
      capture_log([level: :info], fn ->
        FakeTransport.respond(fake, :metadata, metadata_reply(host, %{@topic => [1, 2]}))
        assert_receive {:pin_store, [:kafka_ex, :metadata_store, :recovered], [:duration], []}, 5_000
        refute_receive {:pin_store, [:kafka_ex, :metadata_store, :degraded], _, _}, 300
      end)

    assert recovered_log =~ ~r/\[info\].*Periodic metadata update recovered/
  end

  test "a store restart drops every preferred-replica hint" do
    %{fake: fake, client: client} = start_pinned(api_versions: %{fetch: 11..11})

    FakeTransport.respond(fake, :fetch, fn
      %{node_id: 1} = request -> {:frame, fetch_frame(request, 2)}
      replica -> {:frame, fetch_frame(replica, -1)}
    end)

    {:ok, _} = fetch(client)
    {:ok, _} = fetch(client)

    %{supervisor: supervisor, store: %{pid: store}} = :sys.get_state(client)
    ref = Process.monitor(store)
    Process.exit(store, :kill)
    assert_receive {:DOWN, ^ref, :process, ^store, :killed}, 5_000
    assert {:ok, _ready} = MetadataStore.await_ready(ClientSupervisor.metadata_store(supervisor), 5_000)

    {:ok, _} = fetch(client)

    assert nodes(fake, :fetch) == [1, 2, 1]
  end

  test "a broker that leaves the metadata loses its data connection as well as its metadata connection" do
    %{fake: fake, client: client, host: host} = start_pinned()
    watch_close(host)
    FakeTransport.respond(fake, :produce, produce_reply())
    {:ok, _} = API.produce(client, @topic, 0, [%{value: "v"}])

    FakeTransport.respond(fake, :metadata, Wire.metadata_fields([{2, host, 9093}], %{@topic => [2, 2]}))
    assert {:ok, _} = API.topics_metadata(client, [@topic])

    assert_receive {:pin_closed, %{port: 9092, reason: :metadata_update}}, 5_000
    assert_receive {:pin_closed, %{port: 9092, reason: :metadata_update}}, 5_000
    refute_receive {:pin_closed, _}, 100
  end

  test "a boot whose Metadata reply raises fails start_link with the parser's own exception" do
    Process.flag(:trap_exit, true)
    %{fake: fake, host: host} = start_fake([{1, 9092}, {2, 9093}], api_versions: %{metadata: 12..12})
    FakeTransport.respond(fake, :metadata, &{:frame, null_topics_metadata_frame(&1)})
    expected = parser_exception(null_topics_metadata_frame(%{api_version: 12, correlation_id: 0}))

    {result, _log} =
      with_log(fn ->
        Client.start_link(
          [
            uris: [{host, 9092}],
            consumer_group: :no_consumer_group,
            transport: FakeTransport,
            ssl_options: FakeTransport.ssl_options(fake)
          ],
          :no_name
        )
      end)

    assert {:error, {%module{} = raised, [_ | _]}} = result
    assert module == expected.__struct__
    assert Exception.message(raised) == Exception.message(expected)
  end

  test "the first fetch of an uncached topic caches the replica the leader names" do
    %{fake: fake, client: client, host: host} = start_pinned(api_versions: %{fetch: 11..11}, topics: %{})
    FakeTransport.respond(fake, :metadata, metadata_reply(host, %{@topic => [1, 2]}))

    FakeTransport.respond(fake, :fetch, fn
      %{node_id: 1} = request -> {:frame, fetch_frame(request, 2)}
      replica -> {:frame, fetch_frame(replica, -1)}
    end)

    {:ok, _} = fetch(client)
    {:ok, _} = fetch(client)

    assert nodes(fake, :fetch) == [1, 2]
  end

  # Each round sleeps two ladder delays, so the in-flight round and the queued one together outlast one bound.
  test "a topics_metadata call queued behind a slow in-flight refresh is answered after both rounds" do
    with_request_timeout(50)

    store = [ladder_delay: 700, connect_slack: 40, request_slack: 40, backoff_cap: 10, coordinator_cap: 10]

    %{fake: fake, client: client, host: host} =
      start_pinned(client: [metadata_update_interval: 100, connection: [connect_timeout: 100], store: store])

    one_broker = fn topics -> %{metadata_reply(host, topics) | brokers: [%{node_id: 1, host: host, port: 9092}]} end
    FakeTransport.respond(fake, :metadata, one_broker.(%{@topic => [1], "gone" => [1]}))
    {:ok, [_, _]} = API.topics_metadata(client, [@topic, "gone"], true)

    FakeTransport.respond(fake, :metadata, fn request ->
      if MapSet.member?(StoreCase.requested_topics(request), "late") do
        one_broker.(%{"late" => [1]})
      else
        one_broker.(%{"gone" => [1]})
      end
    end)

    :ok = FakeTransport.subscribe(fake)
    assert_receive {:fake_transport, :request, %{api: :metadata}}, 5_000

    {result, _log} = with_log(fn -> API.topics_metadata(client, ["late"], true) end)

    assert {:ok, [%{name: "late"}]} = result
  end

  defp fetch(client), do: API.fetch(client, @topic, 0, 0, max_wait_time: 10, rack_id: "az-pin")

  defp parser_exception(frame) do
    {response, _rest} = Kayrock.Metadata.V12.Response.deserialize(frame)

    try do
      ResponseParser.metadata_response(response)
      :no_raise
    rescue
      exception -> exception
    end
  end
end
