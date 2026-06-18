defmodule KafkaEx.Client.TransportErrorTest do
  @moduledoc """
  Regression for PR-2 / defect C-1: a transport-level failure (recv-timeout,
  socket closed, ...) must surface its REAL error atom through the client retry
  loop, not be collapsed to `:unknown`.

  Pre-fix, `handle_request_with_retry/4` caught the network result with a
  `{_, _state_out}` wildcard and substituted `Error.build(:unknown, %{})`,
  destroying the real reason before any `retryable?` predicate or the
  consumer-group manager could see it. That relabeling is what turned a
  recv-timeout to the coordinator into a `KafkaEx.SyncGroupError ... :unknown`
  crash in production.
  """
  use ExUnit.Case, async: false
  use Mimic

  import ExUnit.CaptureLog

  alias KafkaEx.Client
  alias KafkaEx.Client.Error
  alias KafkaEx.Client.State
  alias KafkaEx.Cluster.Broker
  alias KafkaEx.Cluster.ClusterMetadata
  alias KafkaEx.Network.NetworkClient
  alias KafkaEx.Network.Socket

  # NOTE: the request runs in THIS process (we call handle_call/3 directly, not
  # via GenServer.call), so private Mimic stubs apply. A future refactor to a real
  # GenServer would need set_mimic_global.
  setup :set_mimic_private

  setup do
    # A genuinely-open loopback socket so Broker.connected?/1 (-> Socket.open?
    # -> Port.info/1) is true WITHOUT stubbing the value module Broker. The
    # socket is never used for I/O — NetworkClient.send_sync_request is stubbed.
    {:ok, listen} = :gen_tcp.listen(0, [:binary, active: false])
    {:ok, lport} = :inet.port(listen)
    {:ok, sock} = :gen_tcp.connect(~c"localhost", lport, [:binary, active: false])

    on_exit(fn ->
      :gen_tcp.close(sock)
      :gen_tcp.close(listen)
    end)

    broker = %Broker{
      node_id: 1,
      host: "localhost",
      port: lport,
      socket: %Socket{socket: sock, ssl: false}
    }

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

  test "recv-timeout surfaces the real :timeout atom, not :unknown", %{state: state} do
    stub(NetworkClient, :send_sync_request, fn _broker, _wire, _timeout -> {:error, :timeout} end)

    {result, log} =
      with_log(fn ->
        Client.handle_call(
          {:offset_fetch, "test-group", [{"t", [%{partition_num: 0}]}], []},
          self(),
          state
        )
      end)

    assert {:reply, {:error, %Error{error: :timeout}}, %State{}} = result
    assert log =~ "failed with error :timeout"
    refute log =~ "failed with error :unknown"
  end
end
