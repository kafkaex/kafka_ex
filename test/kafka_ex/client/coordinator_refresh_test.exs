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

  The `:not_coordinator` is injected via the network seam (PR-2 now surfaces the
  real atom into the retry loop); the observable is that a FindCoordinator request
  gets *built* — which only happens when the client re-discovers the coordinator.
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
end
