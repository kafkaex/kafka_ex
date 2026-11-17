defmodule KafkaEx.Client.ConnectionMapTest do
  use ExUnit.Case, async: false
  use Mimic

  import ExUnit.CaptureLog

  alias KafkaEx.Client
  alias KafkaEx.Client.State
  alias KafkaEx.Cluster.Broker
  alias KafkaEx.Cluster.ClusterMetadata
  alias KafkaEx.Network.NetworkClient
  alias KafkaEx.Network.Socket

  setup :set_mimic_private

  test "reconnecting one bootstrap broker keeps the others" do
    prev = Application.get_env(:kafka_ex, :request_timeout)
    Application.put_env(:kafka_ex, :request_timeout, 50)
    on_exit(fn -> Application.put_env(:kafka_ex, :request_timeout, prev) end)
    # create_socket only: a stubbed send would fail Mimic's type check on node_id: nil
    stub(NetworkClient, :create_socket, fn _h, _p, _s, _u, _a -> open_socket() end)

    state = %State{
      api_versions: %{18 => {0, 3}},
      correlation_id: 1,
      cluster_metadata: %ClusterMetadata{
        brokers: %{
          1 => %Broker{host: "a.test", port: 9092, socket: nil},
          2 => %Broker{host: "b.test", port: 9092, socket: nil}
        }
      }
    }

    {:reply, _, out} =
      capture_log_result(fn -> Client.handle_call({:api_versions, [api_version: 0]}, self(), state) end)

    assert out.cluster_metadata.brokers |> Map.values() |> Enum.map(& &1.host) |> Enum.sort() == ["a.test", "b.test"]
    assert_mirrored(out)
  end

  test "duplicate bootstrap addresses keep one connection and close the other once" do
    {pa, sa1} = responder()
    sa2 = connect(pa)
    Process.put(:metadata_body, metadata_v0_body([{1, pa}]))
    attach_close_events()

    state =
      KafkaEx.TestHelpers.client_state(%State{
        api_versions: %{3 => {0, 0}},
        correlation_id: 1,
        cluster_metadata: %ClusterMetadata{
          brokers: %{
            1 => %Broker{host: "localhost", port: pa, socket: sa1},
            2 => %Broker{host: "localhost", port: pa, socket: sa2}
          }
        }
      })

    {:reply, {:ok, _}, out} = capture_log_result(fn -> Client.handle_call(:update_metadata, self(), state) end)

    kept = out.cluster_metadata.brokers[1].socket
    assert kept in [sa1, sa2]
    assert reasons_for(pa) == [:metadata_update]
    assert [%Socket{socket: orphan}] = [sa1, sa2] -- [kept]
    assert Port.info(orphan) == nil
    assert_mirrored(out)
  end

  test "a remote close drops only that connection" do
    {_p1, s1} = listener()
    {_p2, s2} = listener()

    state =
      KafkaEx.TestHelpers.client_state(%State{
        cluster_metadata: %ClusterMetadata{
          brokers: %{
            1 => %Broker{node_id: 1, host: "h1", port: 1, socket: s1},
            2 => %Broker{node_id: 2, host: "h2", port: 2, socket: s2}
          }
        }
      })

    {:noreply, out} = capture_log_result(fn -> Client.handle_info({:tcp_closed, s1.socket}, state) end)

    assert out.cluster_metadata.brokers[1].socket == nil
    assert out.cluster_metadata.brokers[2].socket == s2
    assert_mirrored(out)
  end

  test "a remote close of a connection without a broker drops it without crashing" do
    {_port, socket} = listener()
    attach_close_events()

    state =
      State.put_connection(
        %State{cluster_metadata: %ClusterMetadata{brokers: %{}}},
        :orphan,
        %{host: "gone", port: 7},
        socket
      )

    log =
      capture_log(fn ->
        {:noreply, out} = Client.handle_info({:tcp_closed, socket.socket}, state)
        send(self(), {:out, out})
      end)

    assert_received {:out, out}
    assert log =~ "gone:7 closed connection"
    assert State.connections(out) == %{}
    assert [%{host: "gone", port: 7, reason: :remote_closed}] = collect_closes()
  end

  test "a late close of a socket already replaced under the same key changes nothing" do
    {_p1, s1} = listener()
    {p2, s2} = listener()
    attach_close_events()

    state =
      %State{cluster_metadata: %ClusterMetadata{brokers: %{}}}
      |> State.put_connection(1, %{host: "h1", port: 1}, s1)
      |> State.put_connection(1, %{host: "h2", port: p2}, s2)

    {:noreply, out} = Client.handle_info({:tcp_closed, s1.socket}, state)

    assert out == state
    assert State.connection_socket(out, 1) == s2
    assert collect_closes() == []
  end

  test "a remote ssl close drops the connection that holds that socket" do
    ref = make_ref()
    other = make_ref()
    attach_close_events()

    state =
      %State{cluster_metadata: %ClusterMetadata{brokers: %{}}}
      |> State.put_connection(:tls, %{host: "tls", port: 9093}, %Socket{socket: ref, ssl: true})
      |> State.put_connection(:other, %{host: "other", port: 9094}, %Socket{socket: other, ssl: true})

    {:noreply, out} = capture_log_result(fn -> Client.handle_info({:ssl_closed, ref}, state) end)

    assert Map.keys(State.connections(out)) == [:other]
    assert [%{host: "tls", port: 9093, reason: :remote_closed}] = collect_closes()
  end

  test "terminate closes once per key, also for keys sharing a socket" do
    {port, socket} = listener()
    attach_close_events()

    state =
      KafkaEx.TestHelpers.client_state(%State{
        cluster_metadata: %ClusterMetadata{
          brokers: %{
            1 => %Broker{node_id: 1, host: "localhost", port: port, socket: socket},
            2 => %Broker{node_id: 2, host: "localhost", port: port, socket: socket}
          }
        }
      })

    Client.terminate(:shutdown, state)

    assert [%{port: ^port, reason: :shutdown}, %{port: ^port, reason: :shutdown}] = collect_closes()
  end

  test "a refresh that moves one node reconnects it and keeps the other" do
    {p1, s1} = listener()
    {p2, s2} = listener()
    {p3, _} = listener()
    attach_close_events()

    stub(NetworkClient, :send_sync_request, fn %Broker{node_id: 1}, _wire, _timeout ->
      metadata_v0([{1, p1}, {2, p3}])
    end)

    state =
      KafkaEx.TestHelpers.client_state(%State{
        api_versions: %{3 => {0, 0}},
        correlation_id: 1,
        cluster_metadata: %ClusterMetadata{
          brokers: %{
            1 => %Broker{node_id: 1, host: "localhost", port: p1, socket: s1},
            2 => %Broker{node_id: 2, host: "localhost", port: p2, socket: s2}
          }
        }
      })

    # Node 2 must not answer the refresh: the stub serves node 1 only.
    {:noreply, state} = capture_log_result(fn -> Client.handle_info({:tcp_closed, s2.socket}, state) end)
    {:reply, {:ok, _}, out} = capture_log_result(fn -> Client.handle_call(:update_metadata, self(), state) end)

    assert out.cluster_metadata.brokers[1].socket == s1
    assert %Socket{} = out.cluster_metadata.brokers[2].socket
    assert out.cluster_metadata.brokers[2].port == p3
    assert reasons_for(p2) == [:remote_closed]
    assert_mirrored(out)
  end

  test "boot metadata moves bootstrap connections to the node at their address without reconnecting" do
    {pa, sa} = responder()
    {pb, sb} = responder()
    Process.put(:metadata_body, metadata_v0_body([{2, pa}, {1, pb}]))
    attach_close_events()
    reject(NetworkClient, :create_socket, 5)

    state =
      KafkaEx.TestHelpers.client_state(%State{
        api_versions: %{3 => {0, 0}},
        correlation_id: 1,
        cluster_metadata: %ClusterMetadata{
          brokers: %{
            1 => %Broker{host: "localhost", port: pa, socket: sa},
            2 => %Broker{host: "localhost", port: pb, socket: sb}
          }
        }
      })

    {:reply, {:ok, _}, out} = capture_log_result(fn -> Client.handle_call(:update_metadata, self(), state) end)

    assert out.cluster_metadata.brokers[2].socket == sa
    assert out.cluster_metadata.brokers[1].socket == sb
    assert out.cluster_metadata.brokers |> Map.values() |> Enum.map(& &1.node_id) |> Enum.sort() == [1, 2]
    assert collect_closes() == []
    assert_mirrored(out)
  end

  test "two nodes swapping addresses keep the sockets at those addresses" do
    {p1, s1} = listener()
    {p2, s2} = listener()
    attach_close_events()
    reject(NetworkClient, :create_socket, 5)

    stub(NetworkClient, :send_sync_request, fn %Broker{}, _wire, _timeout -> metadata_v0([{1, p2}, {2, p1}]) end)

    state =
      KafkaEx.TestHelpers.client_state(%State{
        api_versions: %{3 => {0, 0}},
        correlation_id: 1,
        cluster_metadata: %ClusterMetadata{
          brokers: %{
            1 => %Broker{node_id: 1, host: "localhost", port: p1, socket: s1},
            2 => %Broker{node_id: 2, host: "localhost", port: p2, socket: s2}
          }
        }
      })

    {:reply, {:ok, _}, out} = capture_log_result(fn -> Client.handle_call(:update_metadata, self(), state) end)

    assert out.cluster_metadata.brokers[1].socket == s2
    assert out.cluster_metadata.brokers[2].socket == s1
    assert collect_closes() == []
    assert_mirrored(out)
  end

  test "two nodes at one address share the first key's live socket and the dead one is closed once" do
    {pa, live} = listener()
    dead = connect(pa)
    :ok = NetworkClient.close_socket(%{host: "localhost", port: pa}, dead, :timeout)
    attach_close_events()
    reject(NetworkClient, :create_socket, 5)

    stub(NetworkClient, :send_sync_request, fn %Broker{node_id: 1}, _wire, _timeout ->
      metadata_v0([{1, pa}, {2, pa}])
    end)

    state =
      KafkaEx.TestHelpers.client_state(%State{
        api_versions: %{3 => {0, 0}},
        correlation_id: 1,
        cluster_metadata: %ClusterMetadata{
          brokers: %{
            1 => %Broker{node_id: 1, host: "localhost", port: pa, socket: live},
            2 => %Broker{node_id: 2, host: "localhost", port: pa, socket: dead}
          }
        }
      })

    {:reply, {:ok, _}, out} = capture_log_result(fn -> Client.handle_call(:update_metadata, self(), state) end)

    assert out.cluster_metadata.brokers[1].socket == live
    assert out.cluster_metadata.brokers[2].socket == live
    assert reasons_for(pa) == [:metadata_update]
    assert_mirrored(out)
  end

  defp assert_mirrored(%State{} = state) do
    for {key, broker} <- state.cluster_metadata.brokers do
      assert broker.socket == State.connection_socket(state, key), "broker #{inspect(key)} socket not mirrored"
    end

    assert MapSet.subset?(
             MapSet.new(Map.keys(state.connections)),
             MapSet.new(Map.keys(state.cluster_metadata.brokers))
           )
  end

  defp capture_log_result(fun) do
    parent = self()
    capture_log(fn -> send(parent, {:result, fun.()}) end)
    assert_received {:result, result}
    result
  end

  defp open_socket do
    {_port, socket} = listener()
    socket
  end

  defp listener do
    {:ok, listen} = :gen_tcp.listen(0, [:binary, active: false])
    {:ok, lport} = :inet.port(listen)
    {:ok, sock} = :gen_tcp.connect(~c"localhost", lport, [:binary, active: false])

    on_exit(fn ->
      :gen_tcp.close(sock)
      :gen_tcp.close(listen)
    end)

    {lport, %Socket{socket: sock, ssl: false}}
  end

  # Answers every request with the test process's :metadata_body.
  defp responder do
    test_pid = self()
    {:ok, listen} = :gen_tcp.listen(0, [:binary, packet: 4, active: false, reuseaddr: true])
    {:ok, lport} = :inet.port(listen)
    spawn_link(fn -> accept_loop(listen, test_pid) end)
    on_exit(fn -> :gen_tcp.close(listen) end)
    {lport, connect(lport)}
  end

  defp connect(lport) do
    {:ok, sock} = :gen_tcp.connect(~c"localhost", lport, [:binary, active: false])
    on_exit(fn -> :gen_tcp.close(sock) end)
    %Socket{socket: sock, ssl: false}
  end

  defp accept_loop(listen, test_pid) do
    case :gen_tcp.accept(listen) do
      {:ok, conn} ->
        spawn(fn -> serve(conn, test_pid) end)
        accept_loop(listen, test_pid)

      _ ->
        :ok
    end
  end

  defp serve(conn, test_pid) do
    case :gen_tcp.recv(conn, 0) do
      {:ok, <<_api_key::16, _version::16, correlation_id::32, _::binary>>} ->
        {:dictionary, dict} = Process.info(test_pid, :dictionary)
        :ok = :gen_tcp.send(conn, [<<correlation_id::32>>, Keyword.fetch!(dict, :metadata_body)])
        serve(conn, test_pid)

      _ ->
        :ok
    end
  end

  defp metadata_v0(brokers), do: IO.iodata_to_binary([<<0::32>>, metadata_v0_body(brokers)])

  defp metadata_v0_body(brokers) do
    IO.iodata_to_binary([
      <<length(brokers)::32-signed>>,
      for({id, port} <- brokers, do: [<<id::32-signed>>, string("localhost"), <<port::32-signed>>]),
      <<0::32-signed>>
    ])
  end

  defp string(s), do: <<byte_size(s)::16, s::binary>>

  defp attach_close_events do
    test_pid = self()
    id = "connection-map-#{System.unique_integer([:positive])}"

    :telemetry.attach(id, [:kafka_ex, :connection, :close], fn _, _, md, _ -> send(test_pid, {:close, md}) end, nil)

    on_exit(fn -> :telemetry.detach(id) end)
  end

  defp reasons_for(port), do: for(%{port: ^port, reason: r} <- collect_closes(), do: r)

  defp collect_closes(acc \\ []) do
    receive do
      {:close, md} -> collect_closes([md | acc])
    after
      100 -> Enum.reverse(acc)
    end
  end
end
