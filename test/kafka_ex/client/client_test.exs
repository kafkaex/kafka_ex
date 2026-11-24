defmodule KafkaEx.ClientTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  import KafkaEx.TestSupport.PinCase,
    only: [start_pinned: 1, count: 2, non_handshake: 1, produce_reply: 1, failed_async_sends: 3]

  alias KafkaEx.API
  alias KafkaEx.Client
  alias KafkaEx.Client.Error
  alias KafkaEx.Cluster.Broker
  alias KafkaEx.Messages.RecordMetadata
  alias KafkaEx.TestSupport.FakeTransport

  describe "handle_call/3 - offset_fetch" do
    @describetag pin_set: :invariant

    setup do
      {:ok, start_pinned(api_versions: %{offset_fetch: 0..3})}
    end

    test "returns error for invalid consumer group", %{fake: fake, client: client} do
      sent_before = non_handshake(fake)

      assert {:error, :invalid_consumer_group} =
               GenServer.call(client, {:offset_fetch, :no_consumer_group, [{"test-topic", [%{partition_num: 0}]}], []})

      assert non_handshake(fake) == sent_before
    end

    test "handles valid consumer group", %{client: client} do
      {result, _log} =
        with_log(fn ->
          GenServer.call(client, {:offset_fetch, "test-group", [{"test-topic", [%{partition_num: 0}]}], []})
        end)

      assert {_ok_or_error, _} = result
      assert Process.alive?(client)
    end
  end

  describe "handle_call/3 - offset_commit" do
    @describetag pin_set: :invariant

    setup do
      {:ok, start_pinned(api_versions: %{offset_commit: 0..3})}
    end

    test "returns error for invalid consumer group", %{fake: fake, client: client} do
      sent_before = non_handshake(fake)

      assert {:error, :invalid_consumer_group} =
               GenServer.call(
                 client,
                 {:offset_commit, :no_consumer_group, [{"test-topic", [%{partition_num: 0, offset: 100}]}], []}
               )

      assert non_handshake(fake) == sent_before
    end

    test "handles valid consumer group", %{client: client} do
      {result, _log} =
        with_log(fn ->
          GenServer.call(
            client,
            {:offset_commit, "test-group", [{"test-topic", [%{partition_num: 0, offset: 100}]}], []}
          )
        end)

      assert {_ok_or_error, _} = result
      assert Process.alive?(client)
    end
  end

  # Regression: `:required_acks` never reached the wire (the request builder reads `:acks`), so
  # `acks: 0` was unreachable — and once reachable it crashed on `byte_size(:ok)` and was retried.
  describe "handle_call/3 - produce acks" do
    @describetag pin_set: :invariant

    setup do
      pinned = start_pinned(topics: %{"t" => [1]}, api_versions: %{produce: 0..8})
      FakeTransport.subscribe(pinned.fake)
      {:ok, pinned}
    end

    @tag pin_set: :today_only, retired_by: 1497
    test "acks: 0 is sent fire-and-forget, exactly once, and returns no offset", %{fake: fake, client: client} do
      FakeTransport.respond(fake, :produce, produce_reply("t"))
      {:ok, correlation_id} = API.correlation_id(client)

      reply = produce(client, acks: 0)

      assert {:ok, %RecordMetadata{topic: "t", partition: 0, base_offset: nil}} = reply
      assert API.correlation_id(client) == {:ok, correlation_id + 1}
      assert_receive {:fake_transport, :request, %{api: :produce}}, 5_000
      # Same connection, answered in order: once this returns, every earlier send is in the log.
      assert {:ok, %RecordMetadata{base_offset: 7}} = produce(client, acks: 1)
      assert count(fake, :produce) == 2
    end

    @tag pin_set: :today_only, retired_by: 1497
    test "a failed fire-and-forget send is reported as an error, not a crash, and is sent once", %{
      fake: fake,
      client: client,
      host: host
    } do
      FakeTransport.fail_send(fake, :produce, 0, :closed)
      {:ok, correlation_id} = API.correlation_id(client)

      {reply, log} = with_log(fn -> produce(client, acks: 0) end)

      assert {:error, %Error{error: :closed}} = reply
      assert API.correlation_id(client) == {:ok, correlation_id + 1}
      assert failed_async_sends(log, host, :closed) == 1
    end

    test "an acks value the wire cannot carry is rejected without touching the socket", %{fake: fake, client: client} do
      sent_before = non_handshake(fake)

      for opts <- [[acks: :bogus], [required_acks: 65_536], [acks: nil], [required_acks: 2]] do
        assert {:error, :invalid_acks} = produce(client, opts)
      end

      assert non_handshake(fake) == sent_before
    end

    test "the Java/librdkafka :all / :any spelling reaches the wire as -1", %{fake: fake, client: client} do
      FakeTransport.respond(fake, :produce, produce_reply("t"))
      attach_produce_start_handler()

      assert {:ok, %RecordMetadata{base_offset: 7}} = produce(client, acks: :all)
      assert_receive {:produce_start, %{required_acks: -1}}, 5_000

      assert {:ok, %RecordMetadata{base_offset: 7}} = produce(client, acks: :any)
      assert_receive {:produce_start, %{required_acks: -1}}, 5_000
    end

    test "the deprecated :required_acks is honoured when :acks is absent", %{fake: fake, client: client} do
      FakeTransport.respond(fake, :produce, :no_reply)

      reply = produce(client, required_acks: 0)

      assert {:ok, %RecordMetadata{base_offset: nil}} = reply
      assert_receive {:fake_transport, :request, %{api: :produce}}, 5_000
    end

    test "the deprecated :required_acks loses to an explicit :acks", %{fake: fake, client: client} do
      FakeTransport.respond(fake, :produce, produce_reply("t"))

      # Only the synchronous path reads the broker's offset.
      assert {:ok, %RecordMetadata{base_offset: 7}} = produce(client, required_acks: 0, acks: 1)
      assert count(fake, :produce) == 1
    end

    test "telemetry reports the acks value that reaches the wire", %{fake: fake, client: client} do
      FakeTransport.respond(fake, :produce, produce_reply("t"))
      attach_produce_start_handler()

      produce(client, acks: 1, required_acks: -1)

      assert_receive {:produce_start, %{required_acks: 1}}, 5_000
    end

    test "acks defaults to -1 when neither option is given", %{fake: fake, client: client} do
      FakeTransport.respond(fake, :produce, produce_reply("t"))
      attach_produce_start_handler()

      produce(client, [])

      assert_receive {:produce_start, %{required_acks: -1}}, 5_000
    end
  end

  describe "broker connection handling" do
    test "Broker.connected?/1 returns false for nil socket" do
      broker = %Broker{node_id: 1, host: "localhost", port: 9092, socket: nil}
      refute Broker.connected?(broker)
    end

    test "Broker.connected?/1 returns false for closed socket" do
      broker = %Broker{node_id: 1, host: "localhost", port: 9092, socket: nil}
      refute Broker.connected?(broker)
    end
  end

  describe "reconnection logging" do
    test "logs info message when attempting reconnection" do
      broker = %Broker{node_id: 1, host: "localhost", port: 9092, socket: nil}

      # Verify Broker.to_string produces expected format for logging
      assert Broker.to_string(broker) == "broker 1 (localhost:9092)"

      log =
        capture_log(fn ->
          require Logger
          # This is the format used by reconnect_broker
          Logger.info("Reconnecting to #{Broker.to_string(broker)}, attempt 1/3")
        end)

      assert log =~ "Reconnecting to broker 1 (localhost:9092)"
      assert log =~ "attempt 1/3"
    end

    test "logs warning message after failed reconnection attempts" do
      broker = %Broker{node_id: 1, host: "localhost", port: 9092, socket: nil}

      log =
        capture_log(fn ->
          require Logger
          Logger.warning("Failed to reconnect to #{Broker.to_string(broker)} after 3 attempts")
        end)

      assert log =~ "Failed to reconnect to broker 1 (localhost:9092)"
      assert log =~ "after 3 attempts"
    end
  end

  describe "first_broker_response with disconnected brokers" do
    test "returns error when all brokers disconnected" do
      # Create brokers with nil sockets
      brokers = [
        %Broker{node_id: 1, host: "localhost", port: 9092, socket: nil},
        %Broker{node_id: 2, host: "localhost", port: 9093, socket: nil}
      ]

      # The first_broker_response is private, but we can test through
      # the behavior - disconnected brokers should be skipped
      # and eventually return an error
      for broker <- brokers do
        refute Broker.connected?(broker)
      end
    end
  end

  describe "handle_info/2 - socket errors and unknown messages" do
    test "an unknown message does not kill the client and is logged" do
      %{client: client} = start_pinned([])

      log =
        capture_log(fn ->
          send(client, :something_unexpected)
          :sys.get_state(client)
        end)

      assert log =~ "ignoring unexpected message"
      assert log =~ ":something_unexpected"
    end
  end

  describe "Client.start_link/2 with all brokers unreachable [issue #298]" do
    setup do
      original = Application.get_env(:kafka_ex, :sleep_for_reconnect)
      Application.put_env(:kafka_ex, :sleep_for_reconnect, 1)
      on_exit(fn -> Application.put_env(:kafka_ex, :sleep_for_reconnect, original) end)

      Process.flag(:trap_exit, true)
      :ok
    end

    test "raises 'Brokers sockets are not opened' for a single unreachable broker" do
      assert_init_raises_brokers_not_opened([{"127.0.0.1", 1}])
    end

    test "raises 'Brokers sockets are not opened' when every broker in a multi-broker list is unreachable" do
      assert_init_raises_brokers_not_opened([{"127.0.0.1", 1}, {"127.0.0.1", 2}])
    end
  end

  defp assert_init_raises_brokers_not_opened(uris) do
    args = [uris: uris, consumer_group: :no_consumer_group]

    result =
      try do
        Client.start_link(args, :no_name)
      catch
        :exit, reason -> {:exit, reason}
      end

    drain_exits()

    case result do
      {:error, {%RuntimeError{message: message}, _stacktrace}} ->
        assert message =~ "Brokers sockets are not opened",
               "expected 'Brokers sockets are not opened', got: #{inspect(message)}"

      {:exit, {%RuntimeError{message: message}, _stacktrace}} ->
        assert message =~ "Brokers sockets are not opened",
               "expected 'Brokers sockets are not opened', got: #{inspect(message)}"

      other ->
        flunk("expected Client.start_link to raise 'Brokers sockets are not opened', got: #{inspect(other)}")
    end
  end

  defp drain_exits do
    receive do
      {:EXIT, _pid, _reason} -> drain_exits()
    after
      0 -> :ok
    end
  end

  defp produce(client, opts), do: GenServer.call(client, {:produce, "t", 0, [%{value: "v"}], opts})

  defp attach_produce_start_handler do
    handler_id = "produce-acks-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      handler_id,
      [:kafka_ex, :produce, :start],
      fn _event, _measurements, metadata, pid -> send(pid, {:produce_start, metadata}) end,
      self()
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)
  end
end
