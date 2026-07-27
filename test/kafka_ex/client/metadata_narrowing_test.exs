defmodule KafkaEx.Client.MetadataNarrowingTest do
  @moduledoc """
  Characterizes the tracked-topics-only refresh: a refresh can narrow
  cluster_metadata down to the tracked set, but the effect is bounded to one
  extra metadata round-trip on next use, never permanent data loss.
  """
  use ExUnit.Case, async: false
  use Mimic

  import ExUnit.CaptureLog

  alias KafkaEx.Client
  alias KafkaEx.Client.NodeSelector
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
    # Advertise this ephemeral port in the fake metadata so merge_brokers reuses the
    # existing socket instead of dialing a real broker — keeps the test cluster-free.
    Process.put(:kafkaex_test_broker_port, lport)

    on_exit(fn ->
      :gen_tcp.close(sock)
      :gen_tcp.close(listen)
    end)

    broker = %Broker{node_id: 1, host: "localhost", port: lport, socket: %Socket{socket: sock, ssl: false}}

    base_state = %State{
      cluster_metadata: %ClusterMetadata{brokers: %{1 => broker}},
      api_versions: %{3 => {0, 0}},
      correlation_id: 1,
      tracked_topics: MapSet.new(["t1", "t2", "t3"])
    }

    stub_metadata_response(["t1", "t2", "t3"])

    {{:reply, {:ok, _}, full_state}, _log} =
      capture_log_and_result(fn -> Client.handle_call(:update_metadata, self(), base_state) end)

    # Models "t2/t3 were hit-resolved but never tracked": cm has all three, tracked has only t1.
    state = %{full_state | tracked_topics: MapSet.new(["t1"])}

    {:ok, state: state}
  end

  defp stub_metadata_response(topic_names) do
    stub(NetworkClient, :send_sync_request, fn _broker, _wire, _timeout ->
      build_v0_metadata_response(topic_names)
    end)
  end

  # Hand-rolled V0 wire bytes: Kayrock ships no response serializer.
  defp build_v0_metadata_response(topic_names) do
    partition = [
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
      [<<1::32-signed>>, string("localhost"), <<Process.get(:kafkaex_test_broker_port)::32-signed>>],
      int32_array_len(length(topic_names)),
      topics
    ]
    |> IO.iodata_to_binary()
  end

  defp string(s), do: <<byte_size(s)::16, s::binary>>
  defp int32_array_len(n), do: <<n::32-signed>>
  defp int32_array(values), do: [<<length(values)::32-signed>>, Enum.map(values, &<<&1::32-signed>>)]

  defp capture_log_and_result(fun) do
    log = capture_log(fn -> send(self(), {:capture_log_and_result, fun.()}) end)
    assert_received {:capture_log_and_result, result}
    {result, log}
  end

  test "narrowing: refresh limited to tracked topics drops untracked topics from cluster_metadata", %{state: state} do
    stub_metadata_response(["t1"])

    {{:reply, {:ok, cluster_metadata}, updated_state}, _log} =
      capture_log_and_result(fn -> Client.handle_call(:update_metadata, self(), state) end)

    assert cluster_metadata.topics["t1"]
    refute cluster_metadata.topics["t2"]
    refute cluster_metadata.topics["t3"]

    # The dropped topic forces a metadata round-trip on its next use, not a data loss.
    assert {:error, _} = State.select_broker(updated_state, NodeSelector.topic_partition("t2", 0))
  end

  test "recovery: a topic_metadata call for the dropped topic re-adds it without losing the retained one", %{
    state: state
  } do
    stub_metadata_response(["t1"])

    {{:reply, {:ok, _}, narrowed_state}, _log} =
      capture_log_and_result(fn -> Client.handle_call(:update_metadata, self(), state) end)

    stub_metadata_response(["t1", "t2"])

    {{:reply, {:ok, _topic_metadata}, recovered_state}, _log} =
      capture_log_and_result(fn ->
        Client.handle_call({:topic_metadata, ["t2"], false}, self(), narrowed_state)
      end)

    assert recovered_state.cluster_metadata.topics["t1"]
    assert recovered_state.cluster_metadata.topics["t2"]
    assert recovered_state.tracked_topics == MapSet.new(["t1", "t2"])
  end

  test "convergence: once tracked, a topic survives further refreshes (no thrashing)", %{state: state} do
    stub_metadata_response(["t1"])

    {{:reply, {:ok, _}, narrowed_state}, _log} =
      capture_log_and_result(fn -> Client.handle_call(:update_metadata, self(), state) end)

    stub_metadata_response(["t1", "t2"])

    {{:reply, {:ok, _}, recovered_state}, _log} =
      capture_log_and_result(fn ->
        Client.handle_call({:topic_metadata, ["t2"], false}, self(), narrowed_state)
      end)

    stub_metadata_response(["t1", "t2"])

    {{:reply, {:ok, cluster_metadata}, converged_state}, _log} =
      capture_log_and_result(fn -> Client.handle_call(:update_metadata, self(), recovered_state) end)

    assert cluster_metadata.topics["t1"]
    assert cluster_metadata.topics["t2"]
    assert converged_state.tracked_topics == MapSet.new(["t1", "t2"])
  end
end
