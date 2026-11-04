defmodule KafkaEx.Client.CoordinatorRefreshTest do
  @moduledoc """
  Regression for PR-3 / defect C-3: a NOT_COORDINATOR (or COORDINATOR_NOT_AVAILABLE)
  response on a consumer-group request must invalidate the cached coordinator and
  re-run FindCoordinator before retrying — not blind-retry the stale broker.

  Pre-fix, the metadata-refresh classifier only knew leadership errors, so
  `:not_coordinator` retried the same cached coordinator and FindCoordinator was
  never re-issued (the rolling-deploy hang). Coordinator-refresh classification now
  lives in `Retry.coordinator_refresh_error?/1`. Driven through the real
  `handle_call` path; no private function is exposed.

  The observable in both tests is that a FindCoordinator request gets *built* —
  which only happens when the client re-discovers the coordinator.

  The two tests cover the two ways `:not_coordinator` reaches the retry loop. The
  first injects it at the network seam, where it becomes an `%Error{}`. The second
  is the DescribeGroups API which reports per-group failures as
  `{:error, [{group_id, error_atom}]}`.
  """
  use ExUnit.Case, async: false
  use Mimic

  alias KafkaEx.Client
  alias KafkaEx.Client.RequestBuilder
  alias KafkaEx.Client.State
  alias KafkaEx.Cluster.Broker
  alias KafkaEx.Cluster.ClusterMetadata
  alias KafkaEx.Network.NetworkClient
  alias KafkaEx.Network.Socket

  # Kafka error code 16, NOT_COORDINATOR.
  @not_coordinator 16

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
      consumer_group_for_auto_commit: "test-group",
      api_versions: %{9 => {0, 3}},
      correlation_id: 1,
      cluster_metadata: %ClusterMetadata{
        brokers: %{1 => broker},
        consumer_group_coordinators: %{"test-group" => 1}
      }
    }

    {:ok, state: state}
  end

  test ":not_coordinator on a consumer-group request triggers coordinator re-discovery", %{state: state} do
    test_pid = self()

    stub(NetworkClient, :send_sync_request, fn _broker, _wire, _timeout -> {:error, :not_coordinator} end)

    # FindCoordinator is only built when the client decides to re-discover.
    stub(RequestBuilder, :find_coordinator_request, fn _opts, _state ->
      send(test_pid, :rediscovered)
      {:error, :api_version_no_supported}
    end)

    Client.handle_call(
      {:offset_fetch, "test-group", [{"t", [%{partition_num: 0}]}], []},
      self(),
      state
    )

    # pre-fix this never fires: :not_coordinator retried the stale cached coordinator
    assert_received :rediscovered
  end

  test "a group-level :not_coordinator in a DescribeGroups response triggers re-discovery", %{state: state} do
    test_pid = self()

    stub(NetworkClient, :send_sync_request, fn _broker, _wire, _timeout ->
      describe_groups_v0_response(state.correlation_id, "test-group", @not_coordinator)
    end)

    stub(RequestBuilder, :find_coordinator_request, fn _opts, _state ->
      send(test_pid, :rediscovered)
      {:error, :api_version_no_supported}
    end)

    Client.handle_call({:describe_groups, ["test-group"], [api_version: 0]}, self(), state)

    # pre-fix this never fires: the group-level atom arrived as :unknown
    assert_received :rediscovered
  end

  describe "KIP-848 ConsumerGroupHeartbeat on a connection failure" do
    # Two brokers; the cached coordinator for the group is broker 1, which is
    # gone (every request to it fails at the transport).
    setup %{state: state} do
      [broker] = Map.values(state.cluster_metadata.brokers)

      state = %State{
        state
        | api_versions: %{68 => {0, 1}, 10 => {0, 3}},
          cluster_metadata: %ClusterMetadata{
            brokers: %{1 => broker, 2 => %Broker{broker | node_id: 2}},
            consumer_group_coordinators: %{"test-group" => 1}
          }
      }

      {:ok, state: state}
    end

    for reason <- [:closed, :no_broker] do
      test "#{reason} re-discovers the coordinator before the next attempt", %{state: state} do
        reason = unquote(reason)
        test_pid = self()
        stub(NetworkClient, :send_sync_request, fn _broker, _wire, _timeout -> {:error, reason} end)

        stub(RequestBuilder, :find_coordinator_request, fn _opts, _state ->
          send(test_pid, :rediscovered)
          {:error, :api_version_no_supported}
        end)

        Client.handle_call({:consumer_group_heartbeat, "test-group", [member_id: "m", member_epoch: 5]}, self(), state)

        # pre-fix the client retried the cached coordinator only
        assert_received :rediscovered
      end
    end

    test "a send-once :timeout forgets the coordinator, so the next heartbeat re-discovers it", %{state: state} do
      test_pid = self()
      stub(NetworkClient, :send_sync_request, fn _broker, _wire, _timeout -> {:error, :timeout} end)

      stub(RequestBuilder, :find_coordinator_request, fn _opts, _state ->
        send(test_pid, :rediscovered)
        {:error, :api_version_no_supported}
      end)

      {:reply, {:error, _}, state_out} =
        Client.handle_call({:consumer_group_heartbeat, "test-group", [member_id: "m", member_epoch: 5]}, self(), state)

      refute_received :rediscovered
      assert state_out.cluster_metadata.consumer_group_coordinators == %{}

      Client.handle_call(
        {:consumer_group_heartbeat, "test-group", [member_id: "m", member_epoch: 5]},
        self(),
        state_out
      )

      assert_received :rediscovered
    end

    test "a broker error answer (not a connection failure) keeps the cached coordinator", %{state: state} do
      stub(NetworkClient, :send_sync_request, fn _broker, _wire, _timeout -> {:error, :request_timed_out} end)
      stub(RequestBuilder, :find_coordinator_request, fn _opts, _state -> flunk("re-discovered") end)

      {:reply, {:error, _}, state_out} =
        Client.handle_call({:consumer_group_heartbeat, "test-group", [member_id: "m", member_epoch: 5]}, self(), state)

      assert state_out.cluster_metadata.consumer_group_coordinators == %{"test-group" => 1}
    end

    test "a classic request keeps its behaviour: a :closed is retried on the cached coordinator", %{state: state} do
      stub(NetworkClient, :send_sync_request, fn _broker, _wire, _timeout -> {:error, :closed} end)
      stub(RequestBuilder, :find_coordinator_request, fn _opts, _state -> flunk("re-discovered") end)

      {:reply, {:error, _}, state_out} =
        Client.handle_call(
          {:offset_fetch, "test-group", [{"t", [%{partition_num: 0}]}], [api_version: 3]},
          self(),
          %State{state | api_versions: %{9 => {0, 3}}}
        )

      assert state_out.cluster_metadata.consumer_group_coordinators == %{"test-group" => 1}
    end
  end

  # correlation_id, one group: error_code, group_id, empty state/protocol_type/protocol,
  # no members.
  defp describe_groups_v0_response(correlation_id, group_id, error_code) do
    <<correlation_id::32-signed, 1::32-signed, error_code::16-signed, byte_size(group_id)::16-signed>> <>
      group_id <> <<0::16-signed, 0::16-signed, 0::16-signed, 0::32-signed>>
  end
end
