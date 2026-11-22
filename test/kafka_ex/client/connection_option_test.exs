defmodule KafkaEx.Client.ConnectionOptionTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias KafkaEx.API
  alias KafkaEx.Client
  alias KafkaEx.Client.State
  alias KafkaEx.TestSupport.FakeTransport
  alias KafkaEx.TestSupport.PinCase

  @valid [
    connect_timeout: 300,
    reconnect_window: 0,
    liveness_slack: 0,
    idle_timeout: 1_000,
    idle_jitter: 0.2,
    stable_period: 0,
    send_timeout: 1_000,
    initial_correlation_id: 0,
    client_id: "boot-client"
  ]

  defp start_failure(connection) do
    Process.flag(:trap_exit, true)
    Client.start_link([uris: [{"localhost", 1}], connection: connection], :no_name)
  end

  test "the connection options default to none" do
    assert State.static_init([]).connection_opts == []
  end

  test "the connection options are kept for the connections" do
    opts = [connect_timeout: 300, client_id: "boot-client"]
    assert State.static_init(connection: opts).connection_opts == opts
  end

  test "a :connection that is not a keyword list is rejected" do
    for connection <- [:fast, %{connect_timeout: 300}, [300]] do
      assert {:error, {%ArgumentError{message: message}, _}} = start_failure(connection)
      assert message =~ "invalid :connection"
    end
  end

  defp start_on_fake(connection) do
    Client.start_link(
      [uris: [{"localhost", 9}], consumer_group: :no_consumer_group, transport: FakeTransport, connection: connection],
      :no_name
    )
  end

  test "a client boots with every option a connection reads" do
    assert {:ok, client} = start_on_fake(@valid)
    GenServer.stop(client)
  end

  test "every option a connection reads is accepted at its bounds" do
    Process.flag(:trap_exit, true)
    PinCase.with_env(:sleep_for_reconnect, 0)

    bounds = [
      connect_timeout: 1,
      reconnect_window: 0,
      liveness_slack: 0,
      idle_timeout: 1,
      idle_jitter: 0,
      idle_jitter: 1,
      stable_period: 0,
      send_timeout: 1,
      initial_correlation_id: 0,
      initial_correlation_id: 2_147_483_647
    ]

    for option <- bounds do
      {result, _log} = with_log(fn -> start_on_fake([option]) end)

      refute match?({:error, {%ArgumentError{}, _}}, result), inspect(option)
      with {:ok, client} <- result, do: GenServer.stop(client)
    end
  end

  test "a :connection option a connection reads, of the wrong type or out of range, is rejected" do
    invalid = [
      connect_timeout: 0,
      connect_timeout: :infinity,
      reconnect_window: -1,
      reconnect_window: 1.5,
      liveness_slack: -1,
      idle_timeout: 0,
      idle_timeout: "60000",
      idle_jitter: -0.1,
      idle_jitter: 1.5,
      idle_jitter: :none,
      stable_period: -1,
      send_timeout: 0,
      send_timeout: nil,
      initial_correlation_id: -1,
      initial_correlation_id: 2_147_483_648,
      client_id: nil,
      client_id: :kafka_ex
    ]

    for option <- invalid do
      assert {:error, {%ArgumentError{message: message}, _}} = start_failure([option]), inspect(option)
      assert message =~ "invalid :connection"
    end
  end

  test "a request reads no connection option from the application environment" do
    %{fake: fake, client: client} = PinCase.start_pinned()
    FakeTransport.respond(fake, :produce, PinCase.produce_reply())
    {:ok, _} = API.produce_one(client, "pin", 0, "v")
    trace_env_reads(client)

    {:ok, _} = API.produce_one(client, "pin", 0, "v")

    ref = :erlang.trace_delivered(client)
    assert_receive {:trace_delivered, ^client, ^ref}, 1_000
    refute_received {:trace, ^client, :call, {:application, :get_env, [:kafka_ex, :connect_timeout, _]}}
  end

  defp trace_env_reads(client) do
    :erlang.trace_pattern({:application, :get_env, 3}, true, [:global])
    :erlang.trace(client, true, [:call, {:tracer, self()}])
    on_exit(fn -> :erlang.trace_pattern({:application, :get_env, 3}, false, [:global]) end)
  end
end
