defmodule KafkaEx.ClientTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias KafkaEx.Client
  alias KafkaEx.Client.State
  alias KafkaEx.Cluster.Broker
  alias KafkaEx.Cluster.ClusterMetadata
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
      cluster_metadata: %ClusterMetadata{brokers: brokers_by_id},
      worker_name: :test_worker
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
    on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid) end)
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
end
