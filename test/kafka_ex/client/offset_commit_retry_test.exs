defmodule KafkaEx.Client.OffsetCommitRetryTest do
  @moduledoc """
  Regression for PR-4 / defect H-5: offset_commit must use `commit_retryable?/1` at
  the client retry layer (not the default `always_retryable`), so a rejoin-signal
  error like `:illegal_generation` fails fast (one attempt) instead of burning the
  full client retry budget against a generation the broker has already invalidated.

  Driven through the real `handle_call` path; no private function is exposed.
  """
  use ExUnit.Case, async: false
  use Mimic

  alias KafkaEx.Client
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
      consumer_group_for_auto_commit: "g",
      api_versions: %{8 => {0, 3}},
      correlation_id: 1,
      cluster_metadata: %ClusterMetadata{
        brokers: %{1 => broker},
        consumer_group_coordinators: %{"g" => 1}
      }
    }

    {:ok, state: state}
  end

  test ":illegal_generation on offset_commit is not retried at the client layer", %{state: state} do
    test_pid = self()

    stub(NetworkClient, :send_sync_request, fn _broker, _wire, _timeout ->
      send(test_pid, :sent)
      {:error, :illegal_generation}
    end)

    Client.handle_call(
      {:offset_commit, "g", [{"t", [%{partition_num: 0, offset: 5}]}], []},
      self(),
      state
    )

    # commit_retryable?(:illegal_generation) is false -> exactly one wire attempt.
    # Pre-fix (always_retryable) sent it @retry_count (3) times.
    assert_received :sent
    refute_received :sent
  end
end
