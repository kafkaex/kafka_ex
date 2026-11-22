defmodule KafkaEx.Client.RoutingTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Client.NodeSelector
  alias KafkaEx.Client.Routing

  @selectors [
    NodeSelector.node_id(1),
    NodeSelector.random(),
    NodeSelector.first_available(),
    NodeSelector.controller(),
    NodeSelector.topic_partition("t", 0),
    NodeSelector.topic_partition_replica("t", 0),
    NodeSelector.consumer_group("g")
  ]

  test "metadata, find_coordinator and api_versions go to :metadata whatever the selector" do
    for selector <- @selectors, op <- [:metadata, :find_coordinator, :api_versions] do
      assert Routing.role(selector, op) == :metadata
    end
  end

  test "a consumer group request goes to :coordinator" do
    for op <- [:join_group, :sync_group, :heartbeat, :offset_commit, :offset_fetch, :fetch] do
      assert Routing.role(NodeSelector.consumer_group("g"), op) == :coordinator
    end
  end

  test "a fetch by topic and partition goes to :fetch, one role per broker whatever the partition" do
    assert Routing.role(NodeSelector.topic_partition_replica("orders", 3), :fetch) == :fetch
    assert Routing.role(NodeSelector.topic_partition("orders", 0), :fetch) == :fetch
  end

  test "a fetch with no partition in its selector goes to :data" do
    for selector <- [NodeSelector.node_id(1), NodeSelector.random(), NodeSelector.controller()] do
      assert Routing.role(selector, :fetch) == :data
    end
  end

  test "everything else goes to :data" do
    assert Routing.role(NodeSelector.topic_partition("t", 0), :produce) == :data
    assert Routing.role(NodeSelector.topic_partition("t", 0), :list_offsets) == :data
    assert Routing.role(NodeSelector.controller(), :create_topics) == :data
    assert Routing.role(NodeSelector.node_id(2), :list_groups) == :data
  end
end

defmodule KafkaEx.Client.RoutingConnectLogTest do
  use KafkaEx.TestSupport.PinCase, async: false

  alias KafkaEx.Messages.RecordMetadata

  setup do
    start_pinned(api_versions: %{describe_groups: 0..0})
  end

  test "metadata, find_coordinator and api_versions connect only :metadata connections", %{
    fake: fake,
    client: client
  } do
    assert {:ok, _} = API.topics_metadata(client, [@topic])
    assert {:ok, _} = API.find_coordinator(client, "g")
    assert {:ok, _} = API.api_versions(client)

    assert fake |> FakeTransport.connects() |> Enum.map(& &1.role) |> Enum.uniq() == [:metadata]
  end

  test "no :data connection exists before the first produce, which connects one to the leader", %{
    fake: fake,
    client: client
  } do
    FakeTransport.respond(fake, :produce, produce_reply())
    assert connects(fake, :data) == []

    assert {:ok, %RecordMetadata{base_offset: 7}} = API.produce(client, @topic, 1, [%{value: "v"}])

    assert connects(fake, :data) == [%{role: :data, port: 9093}]
  end

  test "a broker that closes a connection the client used is logged as that broker", %{
    fake: fake,
    client: client,
    host: host
  } do
    watch_close(host)
    FakeTransport.respond(fake, :produce, produce_reply())
    assert {:ok, %RecordMetadata{}} = API.produce(client, @topic, 1, [%{value: "v"}])

    {_, log} =
      with_log([level: :debug], fn ->
        FakeTransport.close(fake, {host, 9093})
        assert_receive {:pin_closed, %{port: 9093, reason: :remote_closed}}, 5_000
      end)

    assert log =~ "broker 2 (#{host}:9093) closed connection"
    refute log =~ ~r/\] #{Regex.escape(host)}:9093 closed connection/
  end

  test "fetches of every partition a broker leads share one :fetch connection to it, never :data", %{
    fake: fake,
    client: client,
    host: host
  } do
    FakeTransport.respond(fake, :metadata, metadata_reply(host, [{@topic, [1, 1, 2]}]))
    assert {:ok, _} = API.topics_metadata(client, [@topic])
    FakeTransport.respond(fake, :fetch, &{:frame, fetch_frame(&1, -1)})

    assert {:ok, _} = API.fetch(client, @topic, 0, 0, max_wait_time: 10)
    assert {:ok, _} = API.fetch(client, @topic, 1, 0, max_wait_time: 10)

    assert connects(fake, :fetch) == [%{role: :fetch, port: 9092}]
    assert connects(fake, :data) == []
  end

  test "group operations share one :coordinator connection to the coordinator", %{fake: fake, client: client} do
    FakeTransport.respond(fake, :heartbeat, %{error_code: 0})
    FakeTransport.respond(fake, :describe_groups, &{:frame, describe_groups_v0_response(&1.correlation_id, "g")})

    assert {:ok, _} = API.describe_group(client, "g")
    assert {:ok, :no_error} = API.heartbeat(client, "g", "m", 1)

    assert connects(fake, :coordinator) == [%{role: :coordinator, port: 9093}]
    assert fake |> requests(:heartbeat) |> Enum.map(&role_of(fake, &1.conn)) |> Enum.uniq() == [:coordinator]
  end

  defp connects(fake, role) do
    for %{role: ^role} = record <- FakeTransport.connects(fake), do: Map.take(record, [:role, :port])
  end

  defp role_of(fake, conn) do
    %{owner: owner} = fake |> FakeTransport.connections() |> Enum.find(&(&1.id == conn))
    fake |> FakeTransport.connects() |> Enum.find(&(&1.owner == owner)) |> Map.fetch!(:role)
  end

  defp describe_groups_v0_response(correlation_id, group_id) do
    <<correlation_id::32-signed, 1::32-signed, 0::16-signed, byte_size(group_id)::16-signed>> <>
      group_id <> <<0::16-signed, 0::16-signed, 0::16-signed, 0::32-signed>>
  end
end
