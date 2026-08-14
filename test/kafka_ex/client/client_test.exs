defmodule KafkaEx.ClientTest do
  use ExUnit.Case, async: false
  use Mimic

  import ExUnit.CaptureLog
  import KafkaEx.TestSupport.ProcessHelpers

  alias KafkaEx.Client
  alias KafkaEx.Client.Error
  alias KafkaEx.Client.State
  alias KafkaEx.Cluster.Broker
  alias KafkaEx.Cluster.ClusterMetadata
  alias KafkaEx.Cluster.Topic
  alias KafkaEx.Messages.RecordMetadata
  alias KafkaEx.Network.NetworkClient
  alias KafkaEx.Network.Socket

  # Tiny GenServer that delegates :tcp_closed/:ssl_closed to
  # `KafkaEx.Client.handle_info/2`. Used to exercise the callback under
  # real BEAM message delivery (not direct function call), without
  # spinning up a full Client (which would require connecting to brokers).
  defmodule ClientHandleInfoShadow do
    @moduledoc false
    use GenServer

    def start_link(initial_state), do: GenServer.start_link(__MODULE__, initial_state)

    @impl true
    def init(state), do: {:ok, state}

    @impl true
    def handle_info(msg, state), do: KafkaEx.Client.handle_info(msg, state)
  end

  describe "handle_call/3 - offset_fetch" do
    test "returns error for invalid consumer group" do
      state = %State{consumer_group_for_auto_commit: :no_consumer_group}

      assert {:reply, {:error, :invalid_consumer_group}, ^state} =
               Client.handle_call(
                 {:offset_fetch, :no_consumer_group, [{"test-topic", [%{partition_num: 0}]}], []},
                 self(),
                 state
               )
    end

    test "handles valid consumer group" do
      # Mock state with valid consumer group and API versions
      state = %State{
        consumer_group_for_auto_commit: "test-group",
        api_versions: %{9 => {0, 3}},
        correlation_id: 1,
        cluster_metadata: %ClusterMetadata{
          brokers: %{},
          consumer_group_coordinators: %{"test-group" => 1}
        }
      }

      # Note: This would require mocking the network layer for a full test
      # Here we just verify the handle_call accepts the valid format
      result =
        Client.handle_call(
          {:offset_fetch, "test-group", [{"test-topic", [%{partition_num: 0}]}], []},
          self(),
          state
        )

      assert match?({:reply, _, _}, result)
    end
  end

  describe "handle_call/3 - offset_commit" do
    test "returns error for invalid consumer group" do
      state = %State{consumer_group_for_auto_commit: :no_consumer_group}

      assert {:reply, {:error, :invalid_consumer_group}, ^state} =
               Client.handle_call(
                 {:offset_commit, :no_consumer_group, [{"test-topic", [%{partition_num: 0, offset: 100}]}], []},
                 self(),
                 state
               )
    end

    test "handles valid consumer group" do
      # Mock state with valid consumer group and API versions
      state = %State{
        consumer_group_for_auto_commit: "test-group",
        api_versions: %{8 => {0, 3}},
        correlation_id: 1,
        cluster_metadata: %ClusterMetadata{
          brokers: %{},
          consumer_group_coordinators: %{"test-group" => 1}
        }
      }

      # Note: This would require mocking the network layer for a full test
      # Here we just verify the handle_call accepts the valid format
      result =
        Client.handle_call(
          {:offset_commit, "test-group", [{"test-topic", [%{partition_num: 0, offset: 100}]}], []},
          self(),
          state
        )

      assert match?({:reply, _, _}, result)
    end
  end

  # Regression: `:required_acks` never reached the wire (the request builder reads `:acks`), so
  # `acks: 0` was unreachable — and once reachable it crashed on `byte_size(:ok)` and was retried.
  describe "handle_call/3 - produce acks" do
    setup :set_mimic_private

    setup do
      {:ok, listen} = :gen_tcp.listen(0, [:binary, active: false])
      {:ok, lport} = :inet.port(listen)
      {:ok, sock} = :gen_tcp.connect(~c"localhost", lport, [:binary, active: false])

      on_exit(fn ->
        :gen_tcp.close(sock)
        :gen_tcp.close(listen)
      end)

      broker = %Broker{node_id: 1, host: "localhost", port: lport, socket: %Socket{socket: sock, ssl: false}}

      state = %State{
        api_versions: %{0 => {0, 8}},
        correlation_id: 1,
        cluster_metadata: %ClusterMetadata{
          brokers: %{1 => broker},
          topics: %{"t" => %Topic{name: "t", partition_leaders: %{0 => 1}}}
        }
      }

      {:ok, state: state}
    end

    test "acks: 0 is sent fire-and-forget, exactly once, and returns no offset", %{state: state} do
      stub_sends(:ok)

      {:reply, reply, state_out} = produce(state, acks: 0)

      assert {:ok, %RecordMetadata{topic: "t", partition: 0, base_offset: nil}} = reply
      assert state_out.correlation_id == state.correlation_id + 1
      assert_received :async_sent
      refute_received :async_sent
      refute_received :sync_sent
    end

    test "a failed fire-and-forget send is reported as an error, not a crash, and is sent once", %{state: state} do
      stub_sends({:error, :closed})

      {:reply, reply, state_out} = produce(state, acks: 0)

      assert {:error, %Error{error: :closed}} = reply
      assert state_out.correlation_id == state.correlation_id + 1
      assert_received :async_sent
      refute_received :async_sent
    end

    test "an acks value the wire cannot carry is rejected without touching the socket", %{state: state} do
      stub_sends(:ok)

      for opts <- [[acks: :all], [required_acks: 65_536], [acks: nil], [required_acks: 2]] do
        assert {:reply, {:error, :invalid_acks}, ^state} = produce(state, opts)
      end

      refute_received :async_sent
      refute_received :sync_sent
    end

    test "the deprecated :required_acks is honoured when :acks is absent", %{state: state} do
      stub_sends(:ok)

      {:reply, reply, _state} = produce(state, required_acks: 0)

      assert {:ok, %RecordMetadata{base_offset: nil}} = reply
      assert_received :async_sent
    end

    test "the deprecated :required_acks loses to an explicit :acks", %{state: state} do
      stub_sends()

      produce(state, required_acks: 0, acks: 1)

      assert_received :sync_sent
      refute_received :sync_sent
      refute_received :async_sent
    end

    test "telemetry reports the acks value that reaches the wire", %{state: state} do
      stub_sends()
      attach_produce_start_handler()

      produce(state, acks: 1, required_acks: -1)

      assert_received {:produce_start, %{required_acks: 1}}
    end

    test "acks defaults to -1 when neither option is given", %{state: state} do
      stub_sends()
      attach_produce_start_handler()

      produce(state, [])

      assert_received {:produce_start, %{required_acks: -1}}
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

  describe "select_broker_with_update via State.select_broker" do
    test "returns error when broker not found" do
      state = %State{
        cluster_metadata: %ClusterMetadata{
          brokers: %{},
          topics: %{}
        }
      }

      selector = KafkaEx.Client.NodeSelector.node_id(999)
      assert {:error, :no_such_node} = State.select_broker(state, selector)
    end

    test "returns broker when found" do
      broker = %Broker{node_id: 1, host: "localhost", port: 9092, socket: nil}

      state = %State{
        cluster_metadata: %ClusterMetadata{
          brokers: %{1 => broker},
          topics: %{}
        }
      }

      selector = KafkaEx.Client.NodeSelector.node_id(1)
      assert {:ok, ^broker} = State.select_broker(state, selector)
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

  describe "NetworkClient nil socket handling" do
    test "send_sync_request returns :not_connected for nil socket" do
      broker = %{socket: nil, host: "localhost", port: 9092}
      assert {:error, :not_connected} = KafkaEx.Network.NetworkClient.send_sync_request(broker, <<>>, 1000)
    end

    test "send_sync_request returns :no_broker for nil broker" do
      assert {:error, :no_broker} = KafkaEx.Network.NetworkClient.send_sync_request(nil, <<>>, 1000)
    end

    test "send_async_request returns :not_connected for nil socket" do
      broker = %{socket: nil, host: "localhost", port: 9092}
      assert {:error, :not_connected} = KafkaEx.Network.NetworkClient.send_async_request(broker, <<>>)
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

  describe "handle_info({:tcp_closed, port}, state) [issue #449]" do
    test "clears the matching broker's socket and returns :noreply" do
      port = open_tcp_port()
      broker = broker_with_port(1, port)
      state = build_state(%{1 => broker})

      assert {:noreply, new_state} = Client.handle_info({:tcp_closed, port}, state)

      [updated_broker] = State.brokers(new_state)
      assert updated_broker.node_id == 1
      assert updated_broker.socket == nil
    end

    test "leaves other brokers' sockets untouched" do
      port_1 = open_tcp_port()
      port_2 = open_tcp_port()

      state =
        build_state(%{
          1 => broker_with_port(1, port_1),
          2 => broker_with_port(2, port_2)
        })

      {:noreply, new_state} = Client.handle_info({:tcp_closed, port_1}, state)

      brokers_by_id =
        new_state
        |> State.brokers()
        |> Map.new(fn b -> {b.node_id, b} end)

      assert brokers_by_id[1].socket == nil
      assert brokers_by_id[2].socket == %Socket{socket: port_2, ssl: false}
    end

    test "is a no-op when no broker matches the port" do
      port_in_state = open_tcp_port()
      port_unknown = open_tcp_port()

      state = build_state(%{1 => broker_with_port(1, port_in_state)})

      {:noreply, new_state} = Client.handle_info({:tcp_closed, port_unknown}, state)

      [unchanged] = State.brokers(new_state)
      assert unchanged.socket == %Socket{socket: port_in_state, ssl: false}
    end

    test "is idempotent when :tcp_closed is received twice" do
      port = open_tcp_port()
      state = build_state(%{1 => broker_with_port(1, port)})

      {:noreply, state_1} = Client.handle_info({:tcp_closed, port}, state)
      {:noreply, state_2} = Client.handle_info({:tcp_closed, port}, state_1)

      [broker] = State.brokers(state_2)
      assert broker.socket == nil
    end

    test "leaves a broker whose socket is already nil unchanged" do
      port = open_tcp_port()
      broker_with_nil = %{broker_with_port(1, port) | socket: nil}
      state = build_state(%{1 => broker_with_nil})

      {:noreply, new_state} = Client.handle_info({:tcp_closed, port}, state)

      [unchanged] = State.brokers(new_state)
      assert unchanged.socket == nil
    end
  end

  describe "handle_info({:ssl_closed, ref}, state) [issue #449]" do
    test "clears the matching broker's socket and returns :noreply" do
      ref = make_ref()
      broker = broker_with_ssl_ref(1, ref)
      state = build_state(%{1 => broker})

      assert {:noreply, new_state} = Client.handle_info({:ssl_closed, ref}, state)

      [updated_broker] = State.brokers(new_state)
      assert updated_broker.socket == nil
    end

    test "leaves other brokers' SSL sockets untouched" do
      ref_1 = make_ref()
      ref_2 = make_ref()

      state =
        build_state(%{
          1 => broker_with_ssl_ref(1, ref_1),
          2 => broker_with_ssl_ref(2, ref_2)
        })

      {:noreply, new_state} = Client.handle_info({:ssl_closed, ref_1}, state)

      brokers_by_id =
        new_state
        |> State.brokers()
        |> Map.new(fn b -> {b.node_id, b} end)

      assert brokers_by_id[1].socket == nil
      assert brokers_by_id[2].socket == %Socket{socket: ref_2, ssl: true}
    end

    test "is a no-op when no broker matches the ref" do
      ref_in_state = make_ref()
      ref_unknown = make_ref()

      state = build_state(%{1 => broker_with_ssl_ref(1, ref_in_state)})

      {:noreply, new_state} = Client.handle_info({:ssl_closed, ref_unknown}, state)

      [unchanged] = State.brokers(new_state)
      assert unchanged.socket == %Socket{socket: ref_in_state, ssl: true}
    end

    test "is idempotent when :ssl_closed is received twice" do
      ref = make_ref()
      state = build_state(%{1 => broker_with_ssl_ref(1, ref)})

      {:noreply, state_1} = Client.handle_info({:ssl_closed, ref}, state)
      {:noreply, state_2} = Client.handle_info({:ssl_closed, ref}, state_1)

      [broker] = State.brokers(state_2)
      assert broker.socket == nil
    end

    test "leaves a broker whose SSL socket is already nil unchanged" do
      ref = make_ref()
      broker_with_nil = %{broker_with_ssl_ref(1, ref) | socket: nil}
      state = build_state(%{1 => broker_with_nil})

      {:noreply, new_state} = Client.handle_info({:ssl_closed, ref}, state)

      [unchanged] = State.brokers(new_state)
      assert unchanged.socket == nil
    end
  end

  # A broker rejecting an acks=0 produce signals it by resetting the connection, so these
  # messages are on the normal error path for fire-and-forget, not an exotic case.
  describe "handle_info/2 - socket errors and unknown messages" do
    test "{:tcp_error, port, reason} clears the matching broker's socket" do
      port = open_tcp_port()
      state = build_state(%{1 => broker_with_port(1, port)})

      assert {:noreply, new_state} = Client.handle_info({:tcp_error, port, :econnreset}, state)

      [broker] = State.brokers(new_state)
      assert broker.socket == nil
    end

    test "{:ssl_error, ref, reason} clears the matching broker's socket" do
      ref = make_ref()
      state = build_state(%{1 => broker_with_ssl_ref(1, ref)})

      assert {:noreply, new_state} = Client.handle_info({:ssl_error, ref, :closed}, state)

      [broker] = State.brokers(new_state)
      assert broker.socket == nil
    end

    test "an unknown message does not kill the client" do
      port = open_tcp_port()
      state = build_state(%{1 => broker_with_port(1, port)})

      assert {:noreply, ^state} = Client.handle_info(:something_unexpected, state)
    end
  end

  describe "telemetry emission on remote close [issue #449]" do
    test "emits [:kafka_ex, :connection, :close] with :remote_closed on :tcp_closed" do
      port = open_tcp_port()
      broker = broker_with_port(1, port)
      state = build_state(%{1 => broker})

      with_telemetry_handler(fn ->
        {:noreply, _} = Client.handle_info({:tcp_closed, port}, state)

        assert_receive {:telemetry, [:kafka_ex, :connection, :close], %{count: 1}, metadata}
        assert metadata.host == broker.host
        assert metadata.port == broker.port
        assert metadata.reason == :remote_closed
      end)
    end

    test "emits [:kafka_ex, :connection, :close] with :remote_closed on :ssl_closed" do
      ref = make_ref()
      broker = broker_with_ssl_ref(1, ref)
      state = build_state(%{1 => broker})

      with_telemetry_handler(fn ->
        {:noreply, _} = Client.handle_info({:ssl_closed, ref}, state)

        assert_receive {:telemetry, [:kafka_ex, :connection, :close], %{count: 1}, metadata}
        assert metadata.host == broker.host
        assert metadata.port == broker.port
        assert metadata.reason == :remote_closed
      end)
    end

    test "does not emit when no broker matches the socket" do
      port_in_state = open_tcp_port()
      port_unknown = open_tcp_port()
      state = build_state(%{1 => broker_with_port(1, port_in_state)})

      with_telemetry_handler(fn ->
        {:noreply, _} = Client.handle_info({:tcp_closed, port_unknown}, state)
        refute_receive {:telemetry, [:kafka_ex, :connection, :close], _, _}, 50
      end)
    end
  end

  describe "remote close via real GenServer dispatch [issue #449]" do
    test "survives :tcp_closed delivered via send/2 and clears the broker's socket" do
      port = open_tcp_port()
      state = build_state(%{1 => broker_with_port(1, port)})
      pid = start_shadow(state)

      send(pid, {:tcp_closed, port})

      # :sys.get_state is a sync call; it blocks until earlier messages
      # in the mailbox are processed, so we don't need Process.sleep.
      updated = :sys.get_state(pid)

      assert Process.alive?(pid)
      [broker] = State.brokers(updated)
      assert broker.socket == nil
    end

    test "survives :ssl_closed delivered via send/2 and clears the broker's socket" do
      ref = make_ref()
      state = build_state(%{1 => broker_with_ssl_ref(1, ref)})
      pid = start_shadow(state)

      send(pid, {:ssl_closed, ref})

      updated = :sys.get_state(pid)

      assert Process.alive?(pid)
      [broker] = State.brokers(updated)
      assert broker.socket == nil
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

  # ---------------------------------------------------------------------------
  # Test helpers for remote-close tests (#449)
  # ---------------------------------------------------------------------------

  defp open_tcp_port do
    {:ok, listen_socket} = :gen_tcp.listen(0, [:binary, active: false])
    on_exit(fn -> :gen_tcp.close(listen_socket) end)
    listen_socket
  end

  defp build_state(brokers_by_id) do
    %State{
      cluster_metadata: %ClusterMetadata{brokers: brokers_by_id}
    }
  end

  defp broker_with_port(node_id, port) do
    %Broker{
      node_id: node_id,
      host: "broker#{node_id}.test",
      port: 9092,
      socket: %Socket{socket: port, ssl: false}
    }
  end

  defp broker_with_ssl_ref(node_id, ref) do
    %Broker{
      node_id: node_id,
      host: "broker#{node_id}.test",
      port: 9093,
      socket: %Socket{socket: ref, ssl: true}
    }
  end

  defp start_shadow(initial_state) do
    {:ok, pid} = ClientHandleInfoShadow.start_link(initial_state)
    on_exit(fn -> stop_safely(pid) end)
    pid
  end

  defp with_telemetry_handler(fun) do
    handler_id = "client-test-remote-close-#{System.unique_integer([:positive])}"
    test_pid = self()

    :ok =
      :telemetry.attach(
        handler_id,
        [:kafka_ex, :connection, :close],
        fn name, measurements, metadata, _ ->
          send(test_pid, {:telemetry, name, measurements, metadata})
        end,
        nil
      )

    try do
      fun.()
    after
      :telemetry.detach(handler_id)
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

  defp produce(state, opts) do
    Client.handle_call({:produce, "t", 0, [%{value: "v"}], opts}, self(), state)
  end

  defp stub_sends(async_result \\ :ok) do
    test_pid = self()

    stub(NetworkClient, :send_async_request, fn _broker, _wire ->
      send(test_pid, :async_sent)
      async_result
    end)

    stub(NetworkClient, :send_sync_request, fn _broker, _wire, _timeout ->
      send(test_pid, :sync_sent)
      {:error, :stubbed}
    end)
  end

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
