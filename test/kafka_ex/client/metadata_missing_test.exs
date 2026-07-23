defmodule KafkaEx.Client.MetadataMissingTest do
  @moduledoc """
  Regression for the metadata give-up path (39a43ed / 7a15d49): a tracked topic
  still missing after `@retry_count` retries must not cause the whole refresh
  to be discarded (the old `{state, nil}` liveness bug), and the missing-topic
  warning must be edge-triggered rather than logged on every refresh cycle.

  Driven through the real `KafkaEx.Client` GenServer path (`handle_call(:update_metadata, ...)`);
  network I/O is stubbed at `NetworkClient.send_sync_request` with hand-built
  V0 Metadata wire responses so real `ResponseParser` parsing is exercised.
  """
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
      cluster_metadata: %ClusterMetadata{brokers: %{1 => broker}},
      # api_key 3 = metadata; {min, max} = {0, 0} negotiates the simplest (V0) wire format.
      api_versions: %{3 => {0, 0}},
      correlation_id: 1,
      tracked_topics: MapSet.new(["present-topic", "missing-topic"])
    }

    {:ok, state: state, broker: broker}
  end

  defp stub_metadata_response(topic_names) do
    stub(NetworkClient, :send_sync_request, fn _broker, _wire, _timeout ->
      build_v0_metadata_response(topic_names)
    end)
  end

  # Hand-rolled Kayrock.Metadata.V0.Response wire bytes (see
  # deps/kayrock/lib/generated/metadata.ex V0.Response.deserialize/1) — Kayrock
  # ships no response serializer since it's a client-only library.
  defp build_v0_metadata_response(topic_names) do
    partition = [
      # error_code, partition_index, leader_id
      <<0::16-signed, 0::32-signed, 1::32-signed>>,
      int32_array([1]),
      int32_array([1])
    ]

    topics =
      Enum.map(topic_names, fn name ->
        [<<0::16-signed>>, string(name), int32_array_len(1), partition]
      end)

    [
      <<1::32-signed>>,
      int32_array_len(1),
      [<<1::32-signed>>, string("localhost"), <<9092::32-signed>>],
      int32_array_len(length(topic_names)),
      topics
    ]
    |> IO.iodata_to_binary()
  end

  defp string(s), do: <<byte_size(s)::16, s::binary>>
  defp int32_array_len(n), do: <<n::32-signed>>
  defp int32_array(values), do: [<<length(values)::32-signed>>, Enum.map(values, &<<&1::32-signed>>)]

  test "give-up merges: missing topic doesn't discard metadata for topics that ARE present", %{state: state} do
    stub_metadata_response(["present-topic"])

    {{:reply, {:ok, cluster_metadata}, updated_state}, _log} =
      capture_log_and_result(fn -> Client.handle_call(:update_metadata, self(), state) end)

    assert %ClusterMetadata{} = cluster_metadata
    assert cluster_metadata.topics["present-topic"]
    refute cluster_metadata.topics["missing-topic"]

    # The liveness bug: give-up used to return {state, nil} and update_metadata
    # discarded the whole refresh, so present-topic's fresh metadata never landed.
    assert updated_state.cluster_metadata.topics["present-topic"]
    assert updated_state.metadata_missing == MapSet.new(["missing-topic"])
  end

  test "warning logged once when a tracked topic first goes missing", %{state: state} do
    stub_metadata_response(["present-topic"])

    log =
      capture_log(fn ->
        Client.handle_call(:update_metadata, self(), state)
      end)

    assert log =~ "[warning]"
    assert log =~ "missing-topic"
    assert log =~ "unavailable"
  end

  test "edge-triggered: repeating the same missing set within the heartbeat window logs no second warning", %{
    state: state
  } do
    stub_metadata_response(["present-topic"])

    {{:reply, _, state_after_first}, first_log} =
      capture_log_and_result(fn -> Client.handle_call(:update_metadata, self(), state) end)

    assert first_log =~ "[warning]"

    {{:reply, _, state_after_second}, second_log} =
      capture_log_and_result(fn -> Client.handle_call(:update_metadata, self(), state_after_first) end)

    refute second_log =~ "[warning]"
    assert state_after_second.metadata_missing == MapSet.new(["missing-topic"])
    assert state_after_second.metadata_missing_logged_at == state_after_first.metadata_missing_logged_at
  end

  test "recovery: topic reappearing logs exactly one info line and no warnings", %{state: state} do
    stub_metadata_response(["present-topic"])

    {{:reply, _, state_after_missing}, _first_log} =
      capture_log_and_result(fn -> Client.handle_call(:update_metadata, self(), state) end)

    stub_metadata_response(["present-topic", "missing-topic"])

    {{:reply, {:ok, cluster_metadata}, recovered_state}, recovery_log} =
      capture_log_and_result(fn -> Client.handle_call(:update_metadata, self(), state_after_missing) end)

    assert cluster_metadata.topics["present-topic"]
    assert cluster_metadata.topics["missing-topic"]

    refute recovery_log =~ "[warning]"
    assert recovery_log =~ "[info]"
    assert recovery_log =~ "recovered"
    assert recovered_state.metadata_missing == MapSet.new()
    assert recovered_state.metadata_missing_logged_at == nil
  end

  defp capture_log_and_result(fun) do
    log = capture_log(fn -> send(self(), {:capture_log_and_result, fun.()}) end)
    assert_received {:capture_log_and_result, result}
    {result, log}
  end
end
