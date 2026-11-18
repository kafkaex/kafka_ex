defmodule KafkaEx.Client.InfraTest do
  use KafkaEx.TestSupport.PinCase, async: false

  alias KafkaEx.Client.Connection
  alias KafkaEx.Client.Infra
  alias KafkaEx.Messages.RecordMetadata

  test "Infra restarts rest-for-one, at most 3 times in 5 seconds, with the connection supervisor first" do
    assert {:ok, {flags, children}} = Infra.init(%{store: %{}})
    assert %{strategy: :rest_for_one, intensity: 3, period: 5} = flags
    assert [%{id: :connections, type: :supervisor}, %{id: :metadata_store}] = children
  end

  test "connection_supervisor/1 finds the running connection supervisor" do
    infra = start_supervised!(Infra)
    sup = Infra.connection_supervisor(infra)
    assert is_pid(sup)
    assert %{active: 0, supervisors: 0} = DynamicSupervisor.count_children(sup)
  end

  test "a stop that runs out of time kills Infra rather than leave it behind" do
    {:ok, infra} = Infra.start_link()
    Process.unlink(infra)
    start_stuck_child(infra)
    monitor = Process.monitor(infra)

    assert Infra.stop(infra, :shutdown, 100) == :ok
    assert_receive {:DOWN, ^monitor, :process, ^infra, _reason}, 1_000
  end

  test "a stop that runs out of time leaves the process linked to Infra alive" do
    {:ok, infra} = Infra.start_link()
    start_stuck_child(infra)
    monitor = Process.monitor(infra)

    assert Infra.stop(infra, :shutdown, 100) == :ok
    assert_receive {:DOWN, ^monitor, :process, ^infra, :killed}, 1_000
  end

  defp start_stuck_child(infra) do
    test = self()

    stuck = fn ->
      Process.flag(:trap_exit, true)
      send(test, :trapping)

      receive do
        :never -> :ok
      end
    end

    {:ok, _pid} =
      DynamicSupervisor.start_child(
        Infra.connection_supervisor(infra),
        %{id: :stuck, start: {Task, :start_link, [stuck]}, shutdown: 5_000}
      )

    assert_receive :trapping, 1_000
  end

  test "a connection is a temporary child with a 2 second shutdown" do
    assert %{restart: :temporary, shutdown: 2_000, start: {Connection, :start_link, [_]}} =
             Supervisor.child_spec({Connection, %{}}, [])
  end

  describe "a running client" do
    setup do
      pinned = start_pinned()
      FakeTransport.respond(pinned.fake, :produce, produce_reply())
      watch_close(pinned.host)
      Map.put(pinned, :infra, :sys.get_state(pinned.client).infra)
    end

    test "is linked to its Infra and does not trap exits", %{client: client, infra: infra} do
      assert {:links, links} = Process.info(client, :links)
      assert infra in links
      assert Process.info(client, :trap_exit) == {:trap_exit, false}
    end

    test "stopped, closes every connection with :shutdown and leaves no Infra or Connection alive", ctx do
      assert_teardown(ctx, fn client ->
        :ok = GenServer.stop(client, :normal, 5_000)
        refute Process.alive?(ctx.infra)
      end)
    end

    test "killed, closes every connection with :shutdown and leaves no Infra or Connection alive", ctx do
      assert_teardown(ctx, &Process.exit(&1, :kill))
    end

    test "survives its connection supervisor being killed, which Infra restarts", %{client: client, infra: infra} do
      supervisor = Infra.connection_supervisor(infra)
      refs = Enum.map([supervisor | connections(infra)], &Process.monitor/1)

      Process.exit(supervisor, :kill)

      for ref <- refs, do: assert_receive({:DOWN, ^ref, :process, _pid, _reason}, 5_000)
      restarted = Infra.connection_supervisor(infra)
      assert is_pid(restarted) and restarted != supervisor
      assert Process.alive?(client)
      assert {:ok, %RecordMetadata{base_offset: 7}} = API.produce(client, @topic, 0, [%{value: "v"}])
    end

    test "answers without a connection while its connection supervisor is down, and connects once it is back", %{
      client: client,
      infra: infra
    } do
      :ok = Supervisor.terminate_child(infra, :connections)
      assert Infra.connection_supervisor(infra) == nil
      {:ok, before} = API.correlation_id(client)
      attach_request_start()

      {result, _log} = with_log(fn -> API.produce(client, @topic, 0, [%{value: "v"}]) end)

      assert result == {:error, :no_broker}
      assert Process.alive?(client)
      assert API.correlation_id(client) == {:ok, before}
      refute_received {:request_start, _metadata}

      {:ok, _supervisor} = Supervisor.restart_child(infra, :connections)
      assert {:ok, %RecordMetadata{base_offset: 7}} = API.produce(client, @topic, 0, [%{value: "v"}])
    end

    test "crashes when a connection fails to start", %{client: client} do
      ref = Process.monitor(client)
      :sys.replace_state(client, &%{&1 | connection_opts: [initial_correlation_id: -1]})

      {exit, _log} = with_log(fn -> catch_exit(API.produce(client, @topic, 0, [%{value: "v"}])) end)

      assert {{{:case_clause, {:error, _}}, [_ | _]}, _call} = exit
      assert_receive {:DOWN, ^ref, :process, ^client, {{:case_clause, {:error, _}}, [_ | _]}}, 5_000
    end

    test "crashes when its connection supervisor exits on a start", %{client: client, infra: infra} do
      ref = Process.monitor(client)
      :sys.replace_state(Infra.connection_supervisor(infra), fn _state -> :broken end)

      {exit, _log} = with_log(fn -> catch_exit(API.produce(client, @topic, 0, [%{value: "v"}])) end)

      assert {{_crash, {GenServer, :call, [_supervisor, {:start_child, _child}, :infinity]}} = reason, _call} = exit
      assert_receive {:DOWN, ^ref, :process, ^client, ^reason}, 5_000
    end
  end

  test "a failed boot leaves no Infra or Connection alive" do
    sleep = Application.get_env(:kafka_ex, :sleep_for_reconnect)
    Application.put_env(:kafka_ex, :sleep_for_reconnect, 0)
    on_exit(fn -> Application.put_env(:kafka_ex, :sleep_for_reconnect, sleep) end)
    Process.flag(:trap_exit, true)
    %{fake: fake, host: host} = start_fake([{1, 9092}])
    before = infra_processes()

    {result, _log} =
      with_log(fn ->
        Client.start_link(
          [
            uris: [{host, 9999}],
            consumer_group: :no_consumer_group,
            transport: FakeTransport,
            ssl_options: FakeTransport.ssl_options(fake)
          ],
          :no_name
        )
      end)

    assert {:error, _reason} = result
    assert [_ | _] = FakeTransport.connects(fake)
    assert infra_processes() -- before == []

    %{client: client} = start_pinned()
    assert client |> :sys.get_state() |> Map.fetch!(:infra) |> connections() |> length() == 1
    assert length(infra_processes() -- before) == 2
  end

  defp assert_teardown(%{client: client, infra: infra}, stop) do
    {:ok, _} = API.produce(client, @topic, 0, [%{value: "v"}])
    {:ok, _} = API.produce(client, @topic, 1, [%{value: "v"}])
    processes = [infra | connections(infra)]
    assert length(processes) == 4
    refs = Enum.map(processes, &Process.monitor/1)

    stop.(client)

    for ref <- refs, do: assert_receive({:DOWN, ^ref, :process, _pid, _reason}, 5_000)
    for _connection <- 1..3, do: assert_receive({:pin_closed, %{reason: :shutdown}}, 1_000)
    refute_receive {:pin_closed, _}, 100
  end

  defp attach_request_start do
    test = self()
    handler = "infra-request-start-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      handler,
      [:kafka_ex, :request, :start],
      fn _event, _measurements, metadata, _config -> send(test, {:request_start, metadata}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)
  end

  defp connections(infra) do
    for {_id, pid, _type, _modules} <- DynamicSupervisor.which_children(Infra.connection_supervisor(infra)),
        is_pid(pid),
        do: pid
  end

  @infra_calls [{:supervisor, Infra, 1}, {Connection, :init, 1}]

  defp infra_processes do
    for pid <- Process.list(),
        {:dictionary, dictionary} <- [Process.info(pid, :dictionary)],
        dictionary[:"$initial_call"] in @infra_calls,
        do: pid
  end
end
