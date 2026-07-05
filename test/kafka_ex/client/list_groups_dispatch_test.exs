defmodule KafkaEx.Client.ListGroupsDispatchTest do
  @moduledoc """
  Regression for the ListGroups final-review Critical finding: a non-zero
  top-level `error_code` in a REAL ListGroups response must flow through
  `handle_request_with_retry/4` as a `%KafkaEx.Client.Error{}`, not a bare
  atom.

  Pre-fix, `ResponseHelpers.parse_response/1` returned `{:error, error_atom}`
  for a non-zero `error_code`. `handle_request_with_retry/4` only matches
  `{:error, [error | _]}` or `{:error, %Error{} = error}`, so a bare atom hit
  no clause and raised `CaseClauseError`, crashing the client `GenServer`.

  This test drives the REAL pipeline: the network layer returns actual wire
  bytes for a `Kayrock.ListGroups.V0.Response{error_code: 15}`, which are
  deserialized by the real Kayrock/KayrockProtocol stack and parsed by the
  real `ResponseHelpers.parse_response/1` before reaching the retry loop --
  unlike the response/response_helpers unit tests, which call the parser
  directly and would not have caught this.
  """
  use ExUnit.Case, async: false
  use Mimic

  alias KafkaEx.Client
  alias KafkaEx.Client.Error
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
      correlation_id: 7,
      cluster_metadata: %ClusterMetadata{brokers: %{1 => broker}}
    }

    {:ok, state: state}
  end

  test "a non-zero top-level error_code from the broker returns %Error{} without crashing", %{
    state: state
  } do
    # Wire bytes for a real Kayrock.ListGroups.V0.Response: correlation_id,
    # error_code = 15 (coordinator_not_available), empty groups array.
    wire_response = <<state.correlation_id::32-signed, 15::16-signed, 0::32-signed>>

    stub(NetworkClient, :send_sync_request, fn _broker, _wire, _timeout -> wire_response end)

    # Pre-fix, ResponseHelpers.parse_response/1 returned a bare atom that hit
    # no clause in handle_request_with_retry/4, raising a CaseClauseError
    # right here. Calling handle_call/3 directly (as Mimic's :set_mimic_private
    # requires) and asserting a clean {:reply, ...} -- with no exception --
    # is exactly what proves the GenServer no longer crashes.
    assert {:reply, result, %State{}} =
             Client.handle_call({:list_groups, 1, [api_version: 0]}, self(), state)

    assert {:error, %Error{error: :coordinator_not_available}} = result
  end
end
