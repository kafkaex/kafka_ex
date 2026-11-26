defmodule KafkaEx.Pins.MessageShapePinTest do
  use KafkaEx.TestSupport.PinCase, async: false

  alias KafkaEx.Client.NodeSelector
  alias KafkaEx.Client.RequestBuilder
  alias KafkaEx.Client.State
  alias KafkaEx.Cluster.Topic
  alias KafkaEx.Config
  alias KafkaEx.Messages.RecordMetadata

  @moduletag pin_set: :invariant

  test "Client.request/3 sends a message to the client and returns its reply" do
    %{fake: fake, client: client} = start_pinned()
    FakeTransport.respond(fake, :produce, produce_reply())

    assert {:ok, %RecordMetadata{}} = Client.request(client, {:produce, @topic, 0, [%{value: "v"}], []}, 10_000)
  end

  describe "new message shapes" do
    test "{:topic_metadata, topics, opts} answers as the old shape does" do
      %{client: client} = start_pinned()

      assert {:ok, [%Topic{name: "pin"}]} =
               Client.request(client, {:topic_metadata, [@topic], [allow_topic_creation: false]}, 5_000)
    end

    test "{:network_request, request, node_selector, opts}" do
      %{client: client} = start_pinned()
      {:ok, request} = RequestBuilder.build(:metadata, [topics: [@topic]], %State{api_versions: versions(client)})

      assert {:ok, %response{}} =
               Client.request(client, {:network_request, request, NodeSelector.first_available(), []}, 5_000)

      assert ["Kayrock", "Metadata", _version, "Response"] = Module.split(response)
    end
  end

  test "a build error returns before any I/O, even when the leader is unreachable" do
    %{fake: fake, client: client, host: host, gate: gate} = start_pinned()
    watch_close(host)
    drop_connection(fake, host)
    PinGate.close(gate)
    watch_connects(host)
    connects = length(FakeTransport.connects(fake))
    started = System.monotonic_time(:millisecond)

    {result, _log} = with_log(fn -> API.produce(client, @topic, 0, [%{value: "v"}], api_version: 99) end)

    assert result == {:error, :api_version_no_supported}
    assert System.monotonic_time(:millisecond) - started < div(Config.request_timeout(), 10)
    assert length(FakeTransport.connects(fake)) == connects
    refute_received {:pin_connecting, _}
    assert count(fake, :produce) == 0
  end

  defp versions(client), do: :sys.get_state(client).api_versions

  # A gated connect never reaches the fake's log; the connection span still starts.
  defp watch_connects(host) do
    test = self()
    handler = "pin-connecting-#{host}"

    :telemetry.attach(
      handler,
      [:kafka_ex, :connection, :start],
      fn _event, _measurements, metadata, _config ->
        if metadata.host == host, do: send(test, {:pin_connecting, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)
  end
end
