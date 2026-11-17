defmodule KafkaEx.Client.StateConnectionsTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Client.State
  alias KafkaEx.Cluster.Broker
  alias KafkaEx.Cluster.ClusterMetadata
  alias KafkaEx.Network.Socket

  defp sock, do: %Socket{socket: make_ref()}

  defp state_with(brokers) do
    map = Map.new(brokers, fn %Broker{node_id: id} = b -> {id, b} end)
    %State{cluster_metadata: %ClusterMetadata{brokers: map}}
  end

  defp broker(id, host, port), do: %Broker{node_id: id, host: host, port: port}

  defp assert_mirror_invariant(%State{cluster_metadata: %{brokers: brokers}} = state) do
    for {key, broker} <- brokers do
      assert broker.socket == State.connection_socket(state, key)
    end
  end

  test "put_connection writes the entry and the mirror; nil and drop clear both" do
    s = sock()
    state = state_with([broker(1, "a", 1), broker(2, "b", 2)])

    state = State.put_connection(state, 1, %{host: "a", port: 1}, s)
    assert State.connection_socket(state, 1) == s
    assert state.cluster_metadata.brokers[1].socket == s
    assert state.cluster_metadata.brokers[2].socket == nil
    assert_mirror_invariant(state)

    cleared = State.put_connection(state, 1, %{host: "a", port: 1}, nil)
    assert State.connection_socket(cleared, 1) == nil
    assert cleared.cluster_metadata.brokers[1].socket == nil

    dropped = State.drop_connection(state, 1)
    assert dropped.connections == %{}
    assert dropped.cluster_metadata.brokers[1].socket == nil
  end

  test "mirror shows a socket only when the entry address equals the broker address" do
    s = sock()
    state = state_with([broker(1, "a", 1)])
    state = State.put_connection(state, 1, %{host: "other", port: 1}, s)

    assert State.connection_socket(state, 1) == s
    assert state.cluster_metadata.brokers[1].socket == nil
    assert [{1, %Broker{socket: nil}}] = State.broker_entries(state)

    assert {:ok, 1, %Broker{socket: nil}} =
             State.select_broker_entry(state, %KafkaEx.Client.NodeSelector{strategy: :node_id, node_id: 1})
  end

  test "reconcile keeps an entry at the same key and address and updates node_id" do
    s = sock()
    state = %{state_with([broker(1, "a", 1)]) | connections: %{1 => %{node_id: nil, host: "a", port: 1, socket: s}}}

    {state, stale} = State.reconcile_connections(state)

    assert stale == []
    assert state.connections[1] == %{node_id: 1, host: "a", port: 1, socket: s}
    assert_mirror_invariant(state)
  end

  test "a new address under the same key is not kept and its old socket is stale" do
    s = sock()
    state = %{state_with([broker(1, "new", 1)]) | connections: %{1 => %{node_id: 1, host: "old", port: 1, socket: s}}}

    {state, stale} = State.reconcile_connections(state)

    assert state.connections == %{}
    assert [%{socket: ^s}] = stale
    assert_mirror_invariant(state)
  end

  test "a new address under the same key is reclaimed by another broker at the old address" do
    s = sock()

    state = %{
      state_with([broker(1, "new", 1), broker(2, "old", 1)])
      | connections: %{1 => %{node_id: 1, host: "old", port: 1, socket: s}}
    }

    {state, stale} = State.reconcile_connections(state)

    assert stale == []
    assert state.connections == %{2 => %{node_id: 2, host: "old", port: 1, socket: s}}
    assert_mirror_invariant(state)
  end

  test "a removed node's entry is stale" do
    s1 = sock()
    s2 = sock()

    state = %{
      state_with([broker(1, "a", 1)])
      | connections: %{
          1 => %{node_id: 1, host: "a", port: 1, socket: s1},
          2 => %{node_id: 2, host: "b", port: 2, socket: s2}
        }
    }

    {state, stale} = State.reconcile_connections(state)

    assert Map.keys(state.connections) == [1]
    assert [%{socket: ^s2}] = stale
  end

  test "bootstrap entries move to the nodes now at their addresses without going stale" do
    sa = sock()
    sb = sock()
    state = state_with([broker(2, "a", 1), broker(1, "b", 2)])

    state = %{
      state
      | connections: %{
          1 => %{node_id: nil, host: "a", port: 1, socket: sa},
          2 => %{node_id: nil, host: "b", port: 2, socket: sb}
        }
    }

    {state, stale} = State.reconcile_connections(state)

    assert stale == []
    assert state.connections[2] == %{node_id: 2, host: "a", port: 1, socket: sa}
    assert state.connections[1] == %{node_id: 1, host: "b", port: 2, socket: sb}
    assert_mirror_invariant(state)
  end

  test "two real nodes swapping addresses keep their sockets by address" do
    sa = sock()
    sb = sock()
    state = state_with([broker(1, "b", 2), broker(2, "a", 1)])

    state = %{
      state
      | connections: %{
          1 => %{node_id: 1, host: "a", port: 1, socket: sa},
          2 => %{node_id: 2, host: "b", port: 2, socket: sb}
        }
    }

    {state, stale} = State.reconcile_connections(state)

    assert stale == []
    assert state.connections[1].socket == sb
    assert state.connections[2].socket == sa
    assert_mirror_invariant(state)
  end

  test "duplicate entries at one address keep one and mark the other stale" do
    s1 = sock()
    s2 = sock()
    state = state_with([broker(1, "a", 1)])

    state = %{
      state
      | connections: %{
          10 => %{node_id: nil, host: "a", port: 1, socket: s1},
          11 => %{node_id: nil, host: "a", port: 1, socket: s2}
        }
    }

    {state, stale} = State.reconcile_connections(state)

    assert [%{socket: kept}] = Map.values(state.connections)
    assert [%{socket: stale_socket}] = stale
    assert Enum.sort([kept, stale_socket]) == Enum.sort([s1, s2])
  end

  test "a socket shared by two keys is not stale while a kept entry holds it" do
    s = sock()
    state = state_with([broker(1, "a", 1)])

    state = %{
      state
      | connections: %{
          1 => %{node_id: 1, host: "a", port: 1, socket: s},
          2 => %{node_id: 2, host: "a", port: 1, socket: s}
        }
    }

    {state, stale} = State.reconcile_connections(state)

    assert stale == []
    assert state.connections[1].socket == s
  end

  test "two new brokers at one address share the old socket" do
    s = sock()

    state = %{
      state_with([broker(1, "h", 1), broker(2, "h", 1)])
      | connections: %{1 => %{node_id: nil, host: "h", port: 1, socket: s}}
    }

    {state, stale} = State.reconcile_connections(state)

    assert stale == []
    assert state.cluster_metadata.brokers[1].socket == s
    assert state.cluster_metadata.brokers[2].socket == s
    assert state.connections[2].node_id == 2
    assert_mirror_invariant(state)
  end

  test "two keys at one address with different sockets both take the first one; the other is stale" do
    s1 = sock()
    s2 = sock()

    state = %{
      state_with([broker(1, "a", 1), broker(2, "a", 1)])
      | connections: %{
          1 => %{node_id: 1, host: "a", port: 1, socket: s1},
          2 => %{node_id: 2, host: "a", port: 1, socket: s2}
        }
    }

    {state, stale} = State.reconcile_connections(state)

    assert state.connections[1] == %{node_id: 1, host: "a", port: 1, socket: s1}
    assert state.connections[2] == %{node_id: 2, host: "a", port: 1, socket: s1}
    assert [%{socket: ^s2}] = stale
    assert_mirror_invariant(state)
  end

  test "find_connections_by_raw_socket matches on the raw socket" do
    s = sock()
    other = sock()

    state = %State{
      connections: %{
        1 => %{node_id: 1, host: "a", port: 1, socket: s},
        2 => %{node_id: 2, host: "b", port: 2, socket: other}
      }
    }

    assert [{1, %{socket: ^s}}] = State.find_connections_by_raw_socket(state, s.socket)
    assert State.find_connections_by_raw_socket(state, make_ref()) == []
  end

  test "client_state seeds connections from broker sockets" do
    s = sock()
    state = state_with([%{broker(1, "a", 1) | socket: s}, broker(2, "b", 2)])

    seeded = KafkaEx.TestHelpers.client_state(state)

    assert seeded.connections |> Map.keys() == [1]
    assert_mirror_invariant(seeded)
  end
end
