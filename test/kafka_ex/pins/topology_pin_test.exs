defmodule KafkaEx.Pins.TopologyPinTest do
  use KafkaEx.TestSupport.PinCase, async: false

  alias KafkaEx.Cluster.Broker
  alias KafkaEx.Messages.RecordMetadata
  alias KafkaEx.TestSupport.FakeTransport.Wire

  @moduletag pin_set: :invariant

  test "brokers from cluster_metadata/1 carry no socket" do
    %{client: client} = start_pinned()

    assert {:ok, %{brokers: brokers}} = API.cluster_metadata(client)
    brokers = Map.values(brokers)

    assert length(brokers) == 2
    assert Enum.all?(brokers, &(&1.socket == nil))
    refute Enum.any?(brokers, &Broker.connected?/1)
  end

  test "a request timeout keeps the connection, and the late reply is dropped" do
    %{fake: fake, client: client, host: host} = start_pinned()
    watch_close(host)
    FakeTransport.respond(fake, :produce, &offset_reply(&1.correlation_id))
    FakeTransport.hold(fake, :produce)
    with_request_timeout(200)

    {timed_out, _log} = with_log(fn -> API.produce(client, @topic, 0, [%{value: "v"}]) end)

    assert timed_out == {:error, :timeout}
    refute_receive {:pin_closed, %{port: 9092}}, 300

    FakeTransport.release(fake, :produce)

    assert {:ok, %RecordMetadata{base_offset: offset}} = API.produce(client, @topic, 0, [%{value: "v"}])
    assert [%{conn: conn}, %{conn: conn, correlation_id: ^offset}] = requests(fake, :produce)
  end

  test "boot opens one :metadata connection, and no data connection exists before its first use" do
    %{fake: fake} = start_pinned()

    assert fake |> FakeTransport.connects() |> Enum.map(&Map.take(&1, [:role, :port])) ==
             [%{role: :metadata, port: 9092}]
  end

  test "one ApiVersions per connect, also under SASL" do
    %{fake: fake, client: client} = start_pinned(client: [auth: oauthbearer()])
    FakeTransport.respond(fake, :produce, produce_reply())

    {:ok, _} = API.produce(client, @topic, 0, [%{value: "v"}])
    {:ok, _} = API.produce(client, @topic, 1, [%{value: "v"}])

    per_conn = fake |> FakeTransport.requests() |> Enum.group_by(& &1.conn, & &1.api)

    assert map_size(per_conn) >= 3
    assert Enum.all?(per_conn, fn {_conn, apis} -> Enum.count(apis, &(&1 == :api_versions)) == 1 end)
  end

  test "a connection is stopped only when no broker advertises its address" do
    %{fake: fake, client: client, host: host} = start_pinned()
    watch_close(host)
    FakeTransport.respond(fake, :produce, produce_reply())
    {:ok, _} = API.produce(client, @topic, 0, [%{value: "v"}])

    FakeTransport.respond(fake, :metadata, metadata([{7, host, 9092}, {2, host, 9093}], [7, 2]))
    assert {:ok, _} = API.topics_metadata(client, [@topic])
    {:ok, _} = API.produce(client, @topic, 0, [%{value: "v"}])

    refute_receive {:pin_closed, %{port: 9092}}, 100
    assert [%{conn: conn}, %{conn: conn}] = requests(fake, :produce)

    FakeTransport.respond(fake, :metadata, metadata([{2, host, 9093}], [2, 2]))
    assert {:ok, _} = API.topics_metadata(client, [@topic])

    assert_receive {:pin_closed, %{port: 9092, reason: :metadata_update}}, 5_000
    refute_receive {:pin_closed, %{port: 9093}}, 100
  end

  defp offset_reply(offset) do
    update_in(
      produce_reply(),
      [:responses, Access.at(0), :partition_responses, Access.at(0)],
      &%{&1 | base_offset: offset}
    )
  end

  defp metadata(brokers, leaders), do: Wire.metadata_fields(brokers, %{@topic => leaders})
end
