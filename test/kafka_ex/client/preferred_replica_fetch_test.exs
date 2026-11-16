defmodule KafkaEx.Client.PreferredReplicaFetchTest do
  @moduledoc false
  use ExUnit.Case, async: false
  use Mimic

  import ExUnit.CaptureLog

  alias KafkaEx.Client
  alias KafkaEx.Client.State
  alias KafkaEx.Cluster.Broker
  alias KafkaEx.Cluster.ClusterMetadata
  alias KafkaEx.Cluster.Topic
  alias KafkaEx.Messages.Fetch
  alias KafkaEx.Network.NetworkClient
  alias KafkaEx.Network.Socket

  @topic "t"
  @leader 1
  @replica 2
  @offset_out_of_range 1

  # handle_call/3 runs in the test process, so private stubs apply.
  setup :set_mimic_private

  setup do
    brokers = Map.new([@leader, @replica], &{&1, open_broker(&1)})

    cluster_metadata = %ClusterMetadata{
      brokers: brokers,
      topics: %{@topic => %Topic{name: @topic, partition_leaders: %{0 => @leader}}}
    }

    {:ok, state: %State{api_versions: %{1 => {0, 11}}, correlation_id: 1, cluster_metadata: cluster_metadata}}
  end

  test "an error from the preferred replica is retried once at the leader", %{state: state} do
    state = with_preferred_replica(state, @replica)
    stub_fetch_errors(%{@replica => @offset_out_of_range, @leader => 0})

    {{:reply, result, new_state}, _log} = with_log(fn -> fetch(state) end)

    assert {:ok, %Fetch{}} = result
    assert_received {:fetched_from, @replica}
    assert_received {:fetched_from, @leader}
    refute_received {:fetched_from, _}
    assert ClusterMetadata.preferred_replica(new_state.cluster_metadata, @topic, 0) == nil
  end

  test "the leader's own error is returned after the fallback", %{state: state} do
    state = with_preferred_replica(state, @replica)
    stub_fetch_errors(%{@replica => @offset_out_of_range, @leader => @offset_out_of_range})

    {{:reply, result, _state}, _log} = with_log(fn -> fetch(state) end)

    assert {:error, %{error: :offset_out_of_range}} = result
    assert_received {:fetched_from, @replica}
    assert_received {:fetched_from, @leader}
    refute_received {:fetched_from, _}
  end

  test "the leader gets a single attempt after the replica's error", %{state: state} do
    state = with_preferred_replica(state, @replica)
    stub_fetch_errors(%{@replica => @offset_out_of_range, @leader => {:error, :closed}})

    {{:reply, result, _state}, _log} = with_log(fn -> fetch(state) end)

    assert {:error, %{error: :closed}} = result
    assert_received {:fetched_from, @replica}
    assert_received {:fetched_from, @leader}
    refute_received {:fetched_from, _}
  end

  test "a retryable error from the replica forgets it without asking the leader in the same call",
       %{state: state} do
    state = with_preferred_replica(state, @replica)
    stub_fetch_errors(%{@replica => {:error, :closed}, @leader => 0})

    {{:reply, result, new_state}, _log} = with_log(fn -> fetch(state) end)

    assert {:error, %{error: :closed}} = result
    refute_received {:fetched_from, @leader}
    assert ClusterMetadata.preferred_replica(new_state.cluster_metadata, @topic, 0) == nil
  end

  test "a leader error without a preferred replica is not sent twice", %{state: state} do
    stub_fetch_errors(%{@leader => @offset_out_of_range})

    {{:reply, result, _state}, _log} = with_log(fn -> fetch(state) end)

    assert {:error, %{error: :offset_out_of_range}} = result
    assert_received {:fetched_from, @leader}
    refute_received {:fetched_from, _}
  end

  defp fetch(state), do: Client.handle_call({:fetch, @topic, 0, 5, [api_version: 0]}, self(), state)

  defp with_preferred_replica(state, node_id) do
    %{state | cluster_metadata: ClusterMetadata.put_preferred_replica(state.cluster_metadata, @topic, 0, node_id)}
  end

  defp stub_fetch_errors(error_by_node) do
    test_pid = self()

    stub(NetworkClient, :send_sync_request, fn %Broker{node_id: node_id}, _wire, _timeout ->
      send(test_pid, {:fetched_from, node_id})

      case Map.fetch!(error_by_node, node_id) do
        {:error, _} = transport_error -> transport_error
        error_code -> fetch_v0_response(error_code)
      end
    end)
  end

  defp fetch_v0_response(error_code) do
    <<1::32-signed, 1::32-signed, byte_size(@topic)::16-signed>> <>
      @topic <> <<1::32-signed, 0::32-signed, error_code::16-signed, 10::64-signed, 0::32-signed>>
  end

  # An open loopback socket makes Broker.connected?/1 true; no I/O goes through it.
  defp open_broker(node_id) do
    {:ok, listen} = :gen_tcp.listen(0, [:binary, active: false])
    {:ok, port} = :inet.port(listen)
    {:ok, sock} = :gen_tcp.connect(~c"localhost", port, [:binary, active: false])

    on_exit(fn ->
      :gen_tcp.close(sock)
      :gen_tcp.close(listen)
    end)

    %Broker{node_id: node_id, host: "localhost", port: port, socket: %Socket{socket: sock, ssl: false}}
  end
end
