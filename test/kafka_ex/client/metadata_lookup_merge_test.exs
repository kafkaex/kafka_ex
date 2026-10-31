defmodule KafkaEx.Client.MetadataLookupMergeTest do
  @moduledoc """
  A targeted `metadata/3` lookup (by name or by id) lists only the requested
  topics. It must return exactly that response, but merge it into the client's
  cached metadata rather than replace it: replacing dropped every other known
  topic, the cached group coordinators, and the open broker sockets (which were
  then leaked, as nothing referenced them any more).

  Driven through the real `Client.handle_call/3` with NetworkClient stubbed.
  """
  use ExUnit.Case, async: false
  use Mimic

  alias KafkaEx.Client
  alias KafkaEx.Client.State
  alias KafkaEx.Cluster.Broker
  alias KafkaEx.Cluster.ClusterMetadata
  alias KafkaEx.Cluster.Topic
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

    socket = %Socket{socket: sock, ssl: false}
    broker = %Broker{node_id: 1, host: "localhost", port: lport, socket: socket}

    state = %State{
      cluster_metadata: %ClusterMetadata{
        brokers: %{1 => broker},
        topics: %{"a" => %Topic{name: "a", partition_leaders: %{0 => 1}}},
        consumer_group_coordinators: %{"g" => 1}
      },
      # api_key 3 = metadata; {0, 0} negotiates the V0 wire format.
      api_versions: %{3 => {0, 0}},
      correlation_id: 1
    }

    stub(NetworkClient, :send_sync_request, fn _broker, _wire, _timeout -> v0_metadata_response(["b"], lport) end)

    {:ok, state: state, socket: socket}
  end

  test "a by-name lookup returns only the requested topic", %{state: state} do
    {:reply, {:ok, %ClusterMetadata{topics: topics}}, _state} =
      Client.handle_call({:metadata, ["b"], [topics: ["b"]], 0}, self(), state)

    assert Map.keys(topics) == ["b"]
  end

  test "...but keeps the other cached topics, the coordinators and the open socket",
       %{state: state, socket: socket} do
    {:reply, {:ok, _}, %State{cluster_metadata: cached}} =
      Client.handle_call({:metadata, ["b"], [topics: ["b"]], 0}, self(), state)

    assert cached.topics |> Map.keys() |> Enum.sort() == ["a", "b"]
    assert cached.consumer_group_coordinators == %{"g" => 1}
    # Before the fix the broker came back with `socket: nil` and the old socket leaked.
    assert %Broker{socket: ^socket} = cached.brokers[1]
  end

  test "a full lookup (no topics filter) replaces the topic map", %{state: state} do
    {:reply, {:ok, _}, %State{cluster_metadata: cached}} =
      Client.handle_call({:metadata, nil, [], 0}, self(), state)

    # The response listed every topic ("b" only), so "a" is gone, as on a refresh.
    assert Map.keys(cached.topics) == ["b"]
    assert cached.consumer_group_coordinators == %{"g" => 1}
  end

  test "the full-lookup check is version-aware: V0 sends [] for all topics, V1+ sends null" do
    # V0's topic array is not nullable, so "all topics" is []; from V1 it is null.
    v0_all = KafkaEx.Protocol.KayrockProtocol.build_request(:metadata, 0, topics: nil, topic_ids: [])
    v1_all = KafkaEx.Protocol.KayrockProtocol.build_request(:metadata, 1, topics: nil, topic_ids: [])
    v1_named = KafkaEx.Protocol.KayrockProtocol.build_request(:metadata, 1, topics: ["b"], topic_ids: [])

    assert v0_all.topics == []
    assert v1_all.topics == nil
    assert [%{name: "b"}] = v1_named.topics
  end

  # Hand-rolled V0 wire bytes: Kayrock ships no response serializer.
  defp v0_metadata_response(topic_names, port) do
    partition = [<<0::16-signed, 0::32-signed, 1::32-signed>>, int32_array([1]), int32_array([1])]

    topics =
      Enum.map(topic_names, fn name -> [<<0::16-signed>>, string(name), <<1::32-signed>>, partition] end)

    [
      <<1::32-signed>>,
      <<1::32-signed>>,
      [<<1::32-signed>>, string("localhost"), <<port::32-signed>>],
      <<length(topic_names)::32-signed>>,
      topics
    ]
    |> IO.iodata_to_binary()
  end

  defp string(s), do: <<byte_size(s)::16, s::binary>>
  defp int32_array(values), do: [<<length(values)::32-signed>>, Enum.map(values, &<<&1::32-signed>>)]
end
