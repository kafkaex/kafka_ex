defmodule KafkaEx.Client.MetadataStoreOutageTest do
  use KafkaEx.TestSupport.PinCase, async: false

  alias KafkaEx.Client.Error
  alias KafkaEx.Client.Infra
  alias KafkaEx.Cluster.Broker

  test "while a restarted store loads, :random selections answer :no_broker and the client stays up" do
    %{client: client, gate: gate} = start_pinned()
    PinGate.close(gate)
    kill_store(client)

    {{metadata, api_versions}, _log} = with_log(fn -> {API.metadata(client), API.api_versions(client)} end)

    assert {:error, %Error{error: :no_broker}} = metadata
    assert {:error, :no_broker} = api_versions
    assert Process.alive?(client)
  end

  test "while a restarted store loads, cluster_metadata/1 keeps answering the last snapshot" do
    %{client: client, gate: gate} = start_pinned()
    {:ok, before} = API.cluster_metadata(client)
    infra = :sys.get_state(client).infra
    PinGate.close(gate)
    kill_store(client)
    assert %{active: 2} = Supervisor.count_children(infra)

    {:ok, during} = API.cluster_metadata(client)
    {:ok, again} = API.cluster_metadata(client)

    assert {during.brokers, during.topics} == {before.brokers, before.topics}
    assert {again.brokers, again.topics} == {before.brokers, before.topics}
  end

  test "a store wait longer than the caller's budget answers :timeout instead of exiting the caller" do
    with_request_timeout(100)
    %{fake: fake, client: client, host: host} = start_pinned(client: [store: [ladder_delay: 4_500]])
    FakeTransport.respond(fake, :metadata, metadata_reply(host, %{@topic => [1, 2]}))

    {result, _log} = with_log(fn -> API.topics_metadata(client, ["ghost"]) end)

    assert result == {:error, :timeout}
    assert Process.alive?(client)
  end

  test "a budget cut inside produce stops its span with the timeout error and emits no exception" do
    with_request_timeout(100)
    %{fake: fake, client: client, host: host} = start_pinned(client: [store: [ladder_delay: 4_500]])
    FakeTransport.respond(fake, :metadata, metadata_reply(host, %{@topic => [1, 2]}))
    test = self()
    handler = "budget-span-#{host}"

    :telemetry.attach_many(
      handler,
      [[:kafka_ex, :produce, :stop], [:kafka_ex, :produce, :exception]],
      fn event, _measurements, metadata, _config -> send(test, {:produce_span, event, metadata}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)

    {result, _log} = with_log(fn -> API.produce(client, "ghost", 0, [%{value: "v"}]) end)

    assert result == {:error, :timeout}

    assert_receive {:produce_span, [:kafka_ex, :produce, :stop], %{result: :error, error: %Error{error: :timeout}}},
                   1_000

    refute_received {:produce_span, [:kafka_ex, :produce, :exception], _}
  end

  test "with no read attempts left, cluster_metadata/1 answers a consistent snapshot through the store" do
    %{fake: fake, client: client} = start_pinned(api_versions: %{fetch: 11..11}, client: [store: [view_attempts: 0]])
    FakeTransport.respond(fake, :heartbeat, %{error_code: 0})

    FakeTransport.respond(fake, :fetch, fn
      %{node_id: 1} = request -> {:frame, fetch_frame(request, 2)}
      replica -> {:frame, fetch_frame(replica, -1)}
    end)

    {:ok, :no_error} = API.heartbeat(client, "g", "m", 1)
    {:ok, _} = API.fetch(client, @topic, 0, 0, max_wait_time: 10, rack_id: "az")

    assert {:ok, metadata} = API.cluster_metadata(client)
    assert metadata.brokers |> Map.keys() |> Enum.sort() == [1, 2]
    assert Map.keys(metadata.topics) == [@topic]
    assert metadata.consumer_group_coordinators == %{"g" => 2}
    assert metadata.preferred_replicas == %{{@topic, 0} => 2}
  end

  test "a cut budget keeps each call's own timeout shape" do
    with_request_timeout(100)
    %{client: client} = start_pinned(api_versions: %{describe_groups: 0..0})
    store = suspend_store(client)

    {results, _log} =
      with_log(fn ->
        {API.describe_group(client, "g"),
         API.list_offsets(client, [{"uncached", [%{partition_num: 0, timestamp: -1}]}])}
      end)

    :sys.resume(store)
    assert {{:error, %Error{error: :timeout}}, {:error, :timeout}} = results
  end

  test "a cut budget still answers topics_metadata/3 for a cached topic from the cache" do
    with_request_timeout(100)
    %{client: client} = start_pinned()
    store = suspend_store(client)

    {result, _log} = with_log(fn -> API.topics_metadata(client, [@topic]) end)

    :sys.resume(store)
    assert {:ok, [%{name: @topic}]} = result
  end

  test "a budget cut inside the metadata span stops it with the timeout error and emits no exception" do
    with_request_timeout(100)
    %{client: client, host: host} = start_pinned()
    watch_span([:kafka_ex, :metadata, :update], host)
    store = suspend_store(client)

    {result, _log} = with_log(fn -> API.metadata(client, ["x"], []) end)

    :sys.resume(store)
    assert {:error, %Error{error: :timeout}} = result

    assert_receive {:span, [:kafka_ex, :metadata, :update, :stop], %{result: :error, error: %Error{error: :timeout}}},
                   1_000

    refute_received {:span, [:kafka_ex, :metadata, :update, :exception], _}
  end

  test "a budget cut at a later candidate of a walk still stops the walk's request span" do
    with_request_timeout(100)
    %{fake: fake, client: client, host: host} = start_pinned()
    watch_span([:kafka_ex, :request], host)
    FakeTransport.hold(fake, :find_coordinator)
    :ok = FakeTransport.subscribe(fake)
    store = :sys.get_state(client).store.pid

    call = Task.async(fn -> with_log(fn -> API.find_coordinator(client, "g") end) end)
    assert_receive {:fake_transport, :request, %{api: :find_coordinator, port: 9092}}, 5_000
    :ok = :sys.suspend(store)

    {result, _log} = Task.await(call, 10_000)
    :sys.resume(store)

    assert result == {:error, :timeout}
    assert_receive {:span, [:kafka_ex, :request, :start], %{operation: :find_coordinator}}, 1_000
    assert_receive {:span, [:kafka_ex, :request, :stop], %{operation: :find_coordinator, error: :timeout}}, 1_000
    refute_received {:span, [:kafka_ex, :request, _], %{operation: :find_coordinator}}
  end

  test "a :network_timeout the budget is not sized by is ignored, not a crash" do
    %{fake: fake, client: client} = start_pinned()
    FakeTransport.respond(fake, :produce, produce_reply())

    assert {:ok, _} = GenServer.call(client, {:produce, @topic, 0, [%{value: "v"}], [network_timeout: 0]}, 10_000)
    assert Process.alive?(client)
  end

  test "a boot whose metadata every bootstrap broker refuses boots with the bootstrap brokers and writes no metadata" do
    previous = Application.fetch_env(:kafka_ex, :api_versions)
    Application.put_env(:kafka_ex, :api_versions, %{metadata: 0})
    on_exit(fn -> restore_env(:api_versions, previous) end)

    {%{fake: fake, client: client, host: host}, log} =
      with_log(fn -> start_pinned(api_versions: %{metadata: 1..1}) end)

    assert {:ok, %{brokers: brokers}} = API.cluster_metadata(client)
    assert brokers == %{1 => %Broker{host: host, port: 9092}}
    assert requests(fake, :metadata) == []
    assert log =~ "failed: :api_version_no_supported"
  end

  test "a topics_metadata/3 refresh every broker refuses at the pinned version returns :api_version_no_supported" do
    previous = Application.fetch_env(:kafka_ex, :api_versions)
    Application.put_env(:kafka_ex, :api_versions, %{metadata: 0})
    on_exit(fn -> restore_env(:api_versions, previous) end)
    {%{fake: fake, client: client}, _log} = with_log(fn -> start_pinned(api_versions: %{metadata: 1..1}) end)

    {result, _log} = with_log(fn -> API.topics_metadata(client, [@topic]) end)

    assert result == {:error, :api_version_no_supported}
    assert requests(fake, :metadata) == []
  end

  test "a cut budget answers cluster_metadata/1 and the internal refresh with the last consistent snapshot" do
    with_request_timeout(100)
    %{client: client} = start_pinned(client: [store: [view_attempts: 0]])
    {:ok, before} = API.cluster_metadata(client)
    store = suspend_store(client)

    {{view, refreshed}, _log} =
      with_log(fn -> {API.cluster_metadata(client), GenServer.call(client, :update_metadata, 10_000)} end)

    :sys.resume(store)
    assert {:ok, %{brokers: brokers, topics: topics}} = view
    assert {brokers, topics} == {before.brokers, before.topics}
    assert map_size(brokers) == 2
    assert {:ok, %{brokers: ^brokers, topics: ^topics}} = refreshed
  end

  test "a torn read whose store call times out answers cluster_metadata/1 with the last consistent snapshot" do
    %{client: client} = start_pinned(client: [store: [view_attempts: 0]])
    {:ok, before} = API.cluster_metadata(client)
    store = suspend_store(client)

    {result, _log} = with_log(fn -> API.cluster_metadata(client) end)

    :sys.resume(store)
    assert {:ok, %{brokers: brokers, topics: topics}} = result
    assert {brokers, topics} == {before.brokers, before.topics}
  end

  test "with the store gone, cluster_metadata/1, the internal refresh and list_groups/2 answer from the last snapshot" do
    %{client: client} = start_pinned(client: [store: [view_attempts: 0]])
    {:ok, before} = API.cluster_metadata(client)
    infra = :sys.get_state(client).infra
    :ok = Supervisor.terminate_child(infra, :metadata_store)

    {{view, refreshed, groups}, _log} =
      with_log(fn ->
        {API.cluster_metadata(client), GenServer.call(client, :update_metadata), API.list_groups(client)}
      end)

    assert {:ok, %{brokers: brokers, topics: topics}} = view
    assert {brokers, topics} == {before.brokers, before.topics}
    assert {:ok, %{brokers: ^brokers}} = refreshed
    assert {:error, {:node_error, 1, _reason}} = groups
  end

  describe "outages and restarts at client level" do
    @describetag pin_set: :invariant

    test "a store killed while every broker refuses: the client and Infra survive, and everything recovers" do
      %{client: client, gate: gate, infra: infra, store: store} = start_outage_client()
      PinGate.close(gate)

      kill(store)
      refute_receive :never, 500

      assert_outage_survived(client, infra)
      PinGate.open(gate)
      assert_recovered(client, infra, store)
    end

    test "a store killed while every broker is black-holed: the client and Infra survive, and everything recovers" do
      %{client: client, fake: fake, infra: infra, store: store} = start_outage_client()
      FakeTransport.respond(fake, :api_versions, :no_reply)

      kill(store)
      refute_receive :never, 500

      assert_outage_survived(client, infra)
      FakeTransport.respond(fake, :api_versions, nil)
      # A load round already hanging on a black-holed handshake gives up after the connect timeout.
      refute_receive :never, 600
      assert_recovered(client, infra, store)
    end

    test "a ConnectionSupervisor killed while every broker refuses: the store restarts with it, and everything recovers" do
      %{client: client, gate: gate, infra: infra, store: store} = start_outage_client()
      PinGate.close(gate)

      kill(Infra.connection_supervisor(infra))
      refute_receive :never, 500

      assert_outage_survived(client, infra)
      PinGate.open(gate)
      assert_recovered(client, infra, store)
    end

    test "a store killed between the client's resolve and its read: the next produce succeeds, the client stays up" do
      %{client: client, infra: infra, store: store} = start_outage_client()
      :ok = :sys.suspend(client)
      request = :gen_server.send_request(client, {:produce, @topic, 0, [%{value: "v"}], []})

      kill(store)
      restarted = Infra.metadata_store(infra)
      assert is_pid(restarted) and restarted != store
      :ok = :sys.resume(client)

      assert {:reply, {:ok, _}} = :gen_server.receive_response(request, 10_000)
      assert Process.alive?(client)
    end
  end

  defp start_outage_client do
    pinned = start_pinned(client: [metadata_update_interval: 100, connection: [connect_timeout: 300]])
    FakeTransport.respond(pinned.fake, :produce, produce_reply())
    %{infra: infra, store: %{pid: store}} = :sys.get_state(pinned.client)
    Map.merge(pinned, %{infra: infra, store: store})
  end

  defp kill(pid) do
    ref = Process.monitor(pid)
    Process.exit(pid, :kill)
    assert_receive {:DOWN, ^ref, :process, ^pid, :killed}, 1_000
  end

  defp assert_outage_survived(client, infra) do
    with_request_timeout(200)
    {during, _log} = with_log(fn -> API.produce(client, @topic, 0, [%{value: "v"}]) end)

    assert {:error, :no_broker} = during
    assert Process.alive?(client)
    assert %{infra: ^infra} = :sys.get_state(client)
    assert Supervisor.count_children(infra).active == 2
  end

  defp assert_recovered(client, infra, store) do
    assert {:ok, _} = API.produce(client, @topic, 0, [%{value: "v"}])
    assert Infra.metadata_store(infra) != store
  end

  defp suspend_store(client) do
    store = :sys.get_state(client).store.pid
    :ok = :sys.suspend(store)
    store
  end

  defp watch_span(prefix, host) do
    test = self()
    handler = "outage-span-#{host}"

    :telemetry.attach_many(
      handler,
      Enum.map([:start, :stop, :exception], &(prefix ++ [&1])),
      fn event, _measurements, metadata, _config -> send(test, {:span, event, metadata}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)
  end

  defp kill_store(client) do
    store = :sys.get_state(client).store.pid
    ref = Process.monitor(store)
    Process.exit(store, :kill)
    assert_receive {:DOWN, ^ref, :process, ^store, :killed}, 5_000
  end

  defp restore_env(key, {:ok, value}), do: Application.put_env(:kafka_ex, key, value)
  defp restore_env(key, :error), do: Application.delete_env(:kafka_ex, key)
end
