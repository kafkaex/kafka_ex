defmodule KafkaEx.Pins.TodayOnlyPinTest do
  use KafkaEx.TestSupport.PinCase, async: false

  alias KafkaEx.Config

  @moduletag pin_set: :today_only

  @tag retired_by: 1492
  test "a raising periodic refresh crashes the client with MetadataUpdateError" do
    Process.flag(:trap_exit, true)

    %{fake: fake, client: client} =
      start_pinned(api_versions: %{metadata: 12..12}, client: [metadata_update_interval: 100])

    Process.link(client)
    # Parsing a v12 reply whose topics array is null raises inside the refresh itself.
    FakeTransport.respond(fake, :metadata, &{:frame, null_topics_metadata_frame(&1)})

    capture_log(fn ->
      assert_receive {:EXIT, ^client, {%KafkaEx.MetadataUpdateError{}, _stacktrace}}, 5_000
    end)
  end

  @tag retired_by: 1492
  test "a failed refresh answers an uncached topic with {:ok, []}" do
    %{fake: fake, client: client} = start_pinned()
    FakeTransport.respond(fake, :metadata, :close)
    with_request_timeout(200)

    {result, _log} = with_log(fn -> API.topics_metadata(client, ["uncached"]) end)

    assert result == {:ok, []}
  end

  @tag retired_by: 1492
  test "topics_metadata/3 of a topic the broker reports unknown returns {:ok, []}" do
    %{fake: fake, client: client, host: host} = start_pinned()
    FakeTransport.respond(fake, :metadata, unknown_topic_reply(host, "gone"))

    {result, _log} = with_log(fn -> API.topics_metadata(client, ["gone"]) end)

    assert result == {:ok, []}
  end

  @tag retired_by: 1492
  test "metadata/3 naming a topic the broker reports unknown returns the metadata without it" do
    %{fake: fake, client: client, host: host} = start_pinned()
    FakeTransport.respond(fake, :metadata, unknown_topic_reply(host, "gone"))

    {result, _log} = with_log(fn -> API.metadata(client, ["gone"], []) end)

    assert {:ok, %KafkaEx.Cluster.ClusterMetadata{topics: topics}} = result
    refute Map.has_key?(topics, "gone")
  end

  @tag retired_by: 1493
  test "the [:kafka_ex, :request] start metadata carries the client-wide correlation id" do
    %{fake: fake, client: client, host: host} = start_pinned()
    FakeTransport.respond(fake, :produce, produce_reply())
    # Moves the client-wide id past any id a fresh node-1 connection would start from.
    for _ <- 1..5, do: {:ok, _} = API.produce(client, @topic, 1, [%{value: "v"}])
    test = self()
    handler = "pin-request-#{host}"

    :telemetry.attach(
      handler,
      [:kafka_ex, :request, :start],
      fn _event, _measurements, metadata, _config -> send(test, {:pin_request, metadata.correlation_id}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)

    {:ok, expected} = API.correlation_id(client)
    {:ok, _} = API.produce(client, @topic, 0, [%{value: "v"}])

    assert_received {:pin_request, ^expected}
  end

  @tag retired_by: 1497
  test ":no_broker is returned well before the request deadline" do
    %{client: client} = start_pinned()
    started = System.monotonic_time(:millisecond)

    {result, _log} = with_log(fn -> API.produce(client, "no-such-topic", 0, [%{value: "v"}]) end)

    assert result == {:error, :no_broker}
    assert System.monotonic_time(:millisecond) - started < div(Config.request_timeout(), 10)
  end

  @tag retired_by: 1497
  test "a known leader whose broker is unreachable returns :no_broker after the reconnect window" do
    with_request_timeout(10_000)
    %{fake: fake, client: client, host: host, gate: gate} = start_pinned()
    watch_close(host)
    drop_connection(fake, host)
    PinGate.close(gate)
    refreshes = count(fake, :metadata)
    started = System.monotonic_time(:millisecond)

    {result, _log} = with_log(fn -> API.produce(client, @topic, 0, [%{value: "v"}]) end)

    assert result == {:error, :no_broker}
    assert System.monotonic_time(:millisecond) - started < 5_000
    assert count(fake, :metadata) == refreshes
  end

  @tag retired_by: 1497
  test "correlation_id/1 counts network attempts" do
    %{fake: fake, client: client} = start_pinned()
    FakeTransport.respond(fake, :produce, produce_reply())
    FakeTransport.respond(fake, :fetch, :close)
    {:ok, before} = API.correlation_id(client)

    {:ok, _} = API.produce(client, @topic, 0, [%{value: "v"}])
    {{:error, :closed}, _log} = with_log(fn -> API.fetch(client, @topic, 0, 0, max_wait_time: 10) end)

    assert {:ok, after_calls} = API.correlation_id(client)
    assert after_calls - before == count(fake, :produce) + count(fake, :fetch)
    assert after_calls - before == 4
  end
end
