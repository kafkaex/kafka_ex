defmodule KafkaEx.Client.RebuildTest do
  use KafkaEx.TestSupport.PinCase, async: false

  alias KafkaEx.Client.NodeSelector
  alias KafkaEx.Client.RequestBuilder
  alias KafkaEx.Client.State
  alias KafkaEx.Messages.RecordMetadata
  alias KafkaEx.Protocol.KayrockProtocol

  @produce_key 0
  @fetch_key 1
  @not_leader_for_partition 6
  @group_authorization_failed 30
  @message_too_large 10

  defp start_mixed(boot_versions, node2_versions) do
    start_pinned(api_versions: boot_versions, fake: [api_versions_by_node: %{2 => node2_versions}])
  end

  defp writes(fake, api), do: fake |> requests(api) |> Enum.map(&{&1.node_id, &1.api_version})

  defp body(%{frame: frame}), do: body(frame)

  defp body(<<_api::16, _version::16, _corr::32, size::16-signed, _client_id::binary-size(size), body::binary>>),
    do: body

  defp expected_body({:ok, request}),
    do:
      %{request | correlation_id: 0, client_id: "c"}
      |> KayrockProtocol.serialize_request()
      |> IO.iodata_to_binary()
      |> body()

  defp at_versions(versions), do: %State{api_versions: versions}

  defp watch_request_spans do
    events = for phase <- [:start, :stop, :exception], do: [:kafka_ex, :request, phase]
    forward_telemetry(events, fn [_, _, phase], _measurements, metadata -> {:span, phase, metadata} end)
  end

  defp with_error_code(reply, code),
    do: put_in(reply, [:responses, Access.at(0), :partition_responses, Access.at(0), :error_code], code)

  describe "a connection that refuses the version the client built" do
    test "a target with a lower max than the boot broker gets the request rebuilt from the caller's options at its own version, at no attempt's cost" do
      %{fake: fake, client: client} = start_mixed(%{produce: 3..3}, %{produce: 2..2})
      FakeTransport.respond(fake, :produce, produce_reply())
      watch_request_spans()
      {:ok, before} = API.correlation_id(client)
      messages = [%{key: "k", value: "v"}]

      assert {:ok, %RecordMetadata{base_offset: 7}} = API.produce(client, @topic, 1, messages, acks: 1, timeout: 1_234)
      assert writes(fake, :produce) == [{2, 2}]
      assert API.correlation_id(client) == {:ok, before + 1}
      assert_receive {:span, :start, %{operation: :produce, api_version: 2}}, 1_000
      assert_receive {:span, :stop, %{operation: :produce, api_version: 2}}, 1_000

      req_data = [topic: @topic, partition: 1, messages: messages, acks: 1, timeout: 1_234]
      expected = req_data |> RequestBuilder.produce_request(at_versions(%{@produce_key => {2, 2}})) |> expected_body()
      assert [written] = requests(fake, :produce)
      assert body(written) == expected
    end

    test "an acks 0 produce is rebuilt at the target's version too" do
      %{fake: fake, client: client} = start_mixed(%{produce: 3..3}, %{produce: 2..2})
      :ok = FakeTransport.subscribe(fake)
      {:ok, before} = API.correlation_id(client)

      assert {:ok, %RecordMetadata{base_offset: nil}} = API.produce(client, @topic, 1, [%{value: "v"}], acks: 0)
      assert_receive {:fake_transport, :request, %{api: :produce}}, 5_000
      assert writes(fake, :produce) == [{2, 2}]
      assert API.correlation_id(client) == {:ok, before + 1}
    end

    test "every attempt of a retried request is written at the target's version, so the rebuild spends no attempt" do
      %{fake: fake, client: client} = start_mixed(%{produce: 3..3}, %{produce: 2..2})

      FakeTransport.respond(fake, :produce, with_error_code(produce_reply(), @not_leader_for_partition))

      {result, log} = with_log(fn -> API.produce(client, @topic, 1, [%{value: "v"}]) end)

      assert {:error, :not_leader_for_partition} = result
      assert writes(fake, :produce) == List.duplicate({2, 2}, Client.retry_count())
      assert log =~ "Request Kayrock.Produce.V2.Request failed with error :not_leader_for_partition"
      refute log =~ "Produce.V3"
    end

    test "the retry loop's log lines name the version written, not the one first built" do
      %{fake: fake, client: client} = start_mixed(%{produce: 3..3}, %{produce: 2..2})
      FakeTransport.respond(fake, :produce, with_error_code(produce_reply(), @message_too_large))

      {result, log} = with_log([level: :info], fn -> API.produce(client, @topic, 1, [%{value: "v"}]) end)

      assert {:error, :message_too_large} = result
      assert log =~ "Error :message_too_large is not retryable for Kayrock.Produce.V2.Request"
      refute log =~ "Produce.V3"
    end

    test "a rebuilt fetch is what RequestBuilder makes from the caller's options at the target's versions" do
      %{fake: fake, client: client} = start_mixed(%{fetch: 11..11}, %{fetch: 0..0})
      FakeTransport.respond(fake, :fetch, fn request -> {:frame, fetch_frame(request, -1)} end)

      assert {:ok, _} = API.fetch(client, @topic, 1, 3, max_wait_time: 123, max_bytes: 4_567)

      req_data = [topic: @topic, partition: 1, offset: 3, max_wait_time: 123, max_bytes: 4_567]
      expected = req_data |> RequestBuilder.fetch_request(at_versions(%{@fetch_key => {0, 0}})) |> expected_body()
      assert [{2, 0}] = writes(fake, :fetch)
      assert [written] = requests(fake, :fetch)
      assert body(written) == expected
    end

    test "a consumer_group_heartbeat rebuild keeps the member id the first build generated" do
      %{fake: fake, client: client} =
        start_mixed(%{consumer_group_heartbeat: 1..1}, %{consumer_group_heartbeat: 0..0})

      FakeTransport.respond(fake, :consumer_group_heartbeat, fn request ->
        {:frame, heartbeat_error_frame(request, @group_authorization_failed)}
      end)

      test = self()
      handler = "rebuild-heartbeat-#{inspect(client)}"
      stop = [:kafka_ex, :consumer, :consumer_group_heartbeat, :stop]
      :telemetry.attach(handler, stop, fn _event, _m, metadata, _c -> send(test, {:heartbeat_stop, metadata}) end, nil)
      on_exit(fn -> :telemetry.detach(handler) end)

      join = [member_epoch: 0, subscribed_topic_names: [@topic], topic_partitions: [], rebalance_timeout_ms: 10_000]
      {result, _log} = with_log(fn -> API.consumer_group_heartbeat(client, "g", join) end)

      assert {:error, _} = result
      assert writes(fake, :consumer_group_heartbeat) == [{2, 0}]
      assert_receive {:heartbeat_stop, %{member_id: reported}}, 1_000
      assert reported != ""
      assert fake |> requests(:consumer_group_heartbeat) |> Enum.map(&member_id/1) == [reported]
    end
  end

  describe "declared change C13: a pinned version outside the target's range" do
    test "an app-config api_versions pin above the target's max is never downgraded" do
      with_env(:api_versions, %{produce: 3})
      %{fake: fake, client: client} = start_mixed(%{produce: 3..3}, %{produce: 2..2})
      FakeTransport.respond(fake, :produce, produce_reply())
      {:ok, before} = API.correlation_id(client)

      {result, _log} = with_log(fn -> API.produce(client, @topic, 1, [%{value: "v"}]) end)

      assert result == {:error, :api_version_no_supported}
      assert requests(fake, :produce) == []
      assert API.correlation_id(client) == {:ok, before}
    end
  end

  describe "an explicit api_version" do
    for {label, boot, target, pinned} <- [
          {"above the boot broker's max is refused before any write, as on master", 2..2, 3..3, 3},
          {"above the target's max is never downgraded (declared change C13)", 3..3, 2..2, 3},
          {"below the target's min is refused after one rebuild (declared change C13)", 2..3, 3..3, 2}
        ] do
      test label do
        %{fake: fake, client: client} =
          start_mixed(%{produce: unquote(Macro.escape(boot))}, %{produce: unquote(Macro.escape(target))})

        FakeTransport.respond(fake, :produce, produce_reply())
        {:ok, before} = API.correlation_id(client)

        {result, _log} =
          with_log(fn -> API.produce(client, @topic, 1, [%{value: "v"}], api_version: unquote(pinned)) end)

        assert result == {:error, :api_version_no_supported}
        assert requests(fake, :produce) == []
        assert API.correlation_id(client) == {:ok, before}
      end
    end
  end

  describe "requests sent without a request context" do
    test "a metadata refresh that walks on to a broker with a lower max is rebuilt at that broker's version" do
      %{fake: fake, client: client, host: host} = start_mixed(%{metadata: 0..1}, %{metadata: 0..0})

      FakeTransport.respond(fake, :metadata, fn request ->
        if request.port == 9092, do: :close, else: metadata_reply(host, [{@topic, [1, 2]}])
      end)

      mark = length(requests(fake, :metadata))
      watch_request_spans()
      {:ok, before} = API.correlation_id(client)
      {result, log} = with_log(fn -> API.topics_metadata(client, [@topic]) end)

      assert {:ok, [%{name: @topic}]} = result
      assert log =~ "Network call to broker 1 (#{host}:9092) failed: :closed\n"
      assert fake |> writes(:metadata) |> Enum.drop(mark) == [{1, 1}, {2, 0}]
      assert_receive {:span, :start, %{operation: :metadata, api_version: 1, correlation_id: ^before}}, 1_000
      assert_receive {:span, :stop, %{operation: :metadata, api_version: 0, broker: %{port: 9093}}}, 1_000
      refute_receive {:span, _, _}, 100
      assert API.correlation_id(client) == {:ok, before + 1}
    end

    test "a coordinator lookup that walks on to a broker with a lower max is rebuilt at that broker's version" do
      %{fake: fake, client: client, host: host} = start_mixed(%{find_coordinator: 1..1}, %{find_coordinator: 0..0})

      FakeTransport.respond(fake, :find_coordinator, fn request ->
        if request.port == 9092, do: :close, else: %{node_id: 2, host: host, port: 9093, error_message: nil}
      end)

      FakeTransport.respond(fake, :heartbeat, %{error_code: 0})

      {result, _log} = with_log(fn -> API.heartbeat(client, "g", "m", 1) end)

      assert {:ok, _} = result
      assert writes(fake, :find_coordinator) == [{1, 1}, {2, 0}]
    end

    test "send_request/4 writes a raw struct for an API the target never advertised, once, and returns its reply" do
      %{fake: fake, client: client} = start_pinned(api_versions: %{heartbeat: nil})
      FakeTransport.respond(fake, :heartbeat, %{error_code: 0})
      request = %Kayrock.Heartbeat.V0.Request{group_id: "g", generation_id: 1, member_id: "m"}

      assert {:ok, %Kayrock.Heartbeat.V0.Response{error_code: 0}} =
               Client.send_request(client, request, NodeSelector.node_id(2))

      assert writes(fake, :heartbeat) == [{2, 0}]
    end

    test "send_request/4 writes a raw struct outside the target's range once, and the broker's close comes back" do
      %{fake: fake, client: client} = start_mixed(%{metadata: 0..1}, %{metadata: 0..0})
      mark = length(requests(fake, :metadata))
      request = %Kayrock.Metadata.V1.Request{topics: [%{name: @topic}]}

      {result, _log} = with_log(fn -> Client.send_request(client, request, NodeSelector.node_id(2)) end)

      assert result == {:error, :closed}
      assert fake |> writes(:metadata) |> Enum.drop(mark) == [{2, 1}]
    end

    test "send_request/4 over first_available writes a raw struct outside every range once per broker" do
      %{fake: fake, client: client} = start_pinned(api_versions: %{metadata: 0..0})
      mark = length(requests(fake, :metadata))
      request = %Kayrock.Metadata.V1.Request{topics: [%{name: @topic}]}

      {result, _log} = with_log(fn -> Client.send_request(client, request, NodeSelector.first_available()) end)

      assert result == {:error, :broker_failed}
      assert fake |> writes(:metadata) |> Enum.drop(mark) |> Enum.sort() == [{1, 1}, {2, 1}]
    end
  end

  defp heartbeat_error_frame(%{correlation_id: corr}, error_code),
    do: <<corr::32-signed, 0, 0::32-signed, error_code::16-signed, 0, 1, -1::32-signed, 0::32-signed, 255, 0>>
end
