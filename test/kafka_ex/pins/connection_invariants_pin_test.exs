defmodule KafkaEx.Pins.ConnectionInvariantsPinTest do
  use KafkaEx.TestSupport.PinCase, async: false

  alias KafkaEx.Auth.Config, as: AuthConfig
  alias KafkaEx.Messages.RecordMetadata

  @moduletag pin_set: :invariant

  @span_events [
    [:kafka_ex, :connection, :start],
    [:kafka_ex, :connection, :stop],
    [:kafka_ex, :auth, :start],
    [:kafka_ex, :auth, :stop]
  ]

  describe "on a running client" do
    setup do
      start_pinned()
    end

    test "coordinator requests are written in the order they reach the client", %{fake: fake, client: client} do
      FakeTransport.respond(fake, :heartbeat, %{error_code: 0})

      refs =
        for generation <- 1..3 do
          ref = make_ref()
          send(client, {:"$gen_call", {self(), ref}, {:heartbeat, "g", "m", generation, []}})
          ref
        end

      for ref <- refs, do: assert_receive({^ref, {:ok, :no_error}}, 5_000)
      assert fake |> requests(:heartbeat) |> Enum.map(&generation/1) == [1, 2, 3]
    end

    test "a produce reply that arrives after its request timed out never answers the next produce", %{
      fake: fake,
      client: client
    } do
      with_request_timeout(200)
      FakeTransport.respond(fake, :produce, fn request -> offset_reply(request.correlation_id) end)
      FakeTransport.hold(fake, :produce)

      {timed_out, _log} = with_log(fn -> API.produce(client, @topic, 0, [%{value: "v"}]) end)
      FakeTransport.release(fake, :produce)
      {answered, _log} = with_log(fn -> API.produce(client, @topic, 0, [%{value: "v"}]) end)

      assert timed_out == {:error, :timeout}
      assert {:ok, %RecordMetadata{base_offset: offset}} = answered
      assert offset == List.last(requests(fake, :produce)).correlation_id
    end

    test "a fetch long-poll that outlasts :request_timeout is answered without closing its socket", %{
      fake: fake,
      client: client,
      host: host
    } do
      with_request_timeout(200)
      watch_close(host)
      FakeTransport.respond(fake, :fetch, &{:frame, fetch_frame(&1, -1)})
      FakeTransport.subscribe(fake)
      FakeTransport.hold(fake, :fetch)

      fetch = Task.async(fn -> API.fetch(client, @topic, 0, 0, max_wait_time: 1_000) end)
      assert_receive {:fake_transport, :request, %{api: :fetch}}, 5_000
      refute_receive {:pin_closed, _}, 500
      FakeTransport.release(fake, :fetch)

      assert {:ok, _} = Task.await(fetch, 5_000)
      assert count(fake, :fetch) == 1
    end
  end

  describe "version precedence under a negotiated produce range of 2..3" do
    setup do
      pinned = start_pinned(api_versions: %{produce: 2..3})
      FakeTransport.respond(pinned.fake, :produce, produce_reply())
      pinned
    end

    test "a per-request api_version is written as that version", %{fake: fake, client: client} do
      assert {:ok, _} = API.produce(client, @topic, 0, [%{value: "v"}], api_version: 2)
      assert List.last(requests(fake, :produce)).api_version == 2
    end

    test "an application-config version is written when the request names none", %{fake: fake, client: client} do
      previous = Application.fetch_env(:kafka_ex, :api_versions)
      Application.put_env(:kafka_ex, :api_versions, %{produce: 2})
      on_exit(fn -> restore_env(:api_versions, previous) end)

      assert {:ok, _} = API.produce(client, @topic, 0, [%{value: "v"}])
      assert List.last(requests(fake, :produce)).api_version == 2
    end

    test "a version above the negotiated max is refused without a write", %{fake: fake, client: client} do
      before = count(fake, :produce)

      {result, _log} = with_log(fn -> API.produce(client, @topic, 0, [%{value: "v"}], api_version: 4) end)

      assert result == {:error, :api_version_no_supported}
      assert count(fake, :produce) == before
    end
  end

  describe "connect telemetry" do
    test "every connection span closes with success and the measured metadata keys" do
      attach_spans()
      %{fake: fake, client: client, host: host} = start_pinned()
      FakeTransport.respond(fake, :produce, produce_reply())
      assert {:ok, _} = API.produce(client, @topic, 0, [%{value: "v"}])

      spans = spans(host)

      assert spans != []
      assert Enum.all?(spans, &(&1.event == :connection))

      for span <- spans do
        assert span.stop != nil
        assert keys(span.start) == [:host, :port, :ssl]
        assert keys(span.stop) == [:success]
        assert span.stop.success == true
      end
    end

    test "every OAUTHBEARER connection span encloses exactly one auth span" do
      attach_spans()
      auth = AuthConfig.new(%{mechanism: :oauthbearer, mechanism_opts: %{token_provider: fn -> {:ok, "t"} end}})
      %{fake: fake, client: client, host: host} = start_pinned(client: [auth: auth])
      FakeTransport.respond(fake, :produce, produce_reply())
      assert {:ok, _} = API.produce(client, @topic, 0, [%{value: "v"}])

      spans = spans(host)
      connections = Enum.filter(spans, &(&1.event == :connection))

      assert connections != []

      for connection <- connections do
        assert [auth_span] = Enum.filter(spans, &(&1.event == :auth and encloses?(connection, &1)))
        assert auth_span.start.mechanism == "OAUTHBEARER"
        assert keys(auth_span.start) == [:host, :mechanism, :port]
        assert keys(auth_span.stop) == []
      end

      assert Enum.count(spans, &(&1.event == :auth)) == length(connections)
    end
  end

  defp generation(%{frame: frame}) do
    <<_::64, client_id::16-signed, _::binary-size(client_id), group::16-signed, _::binary-size(group),
      generation::32-signed, _::binary>> = frame

    generation
  end

  defp offset_reply(corr) do
    update_in(
      produce_reply(),
      [:responses, Access.at(0), :partition_responses, Access.at(0)],
      &%{&1 | base_offset: corr}
    )
  end

  defp restore_env(key, {:ok, value}), do: Application.put_env(:kafka_ex, key, value)
  defp restore_env(key, :error), do: Application.delete_env(:kafka_ex, key)

  defp attach_spans do
    handler = "pin-spans-#{System.unique_integer([:positive])}"
    :telemetry.attach_many(handler, @span_events, &__MODULE__.forward_span/4, self())
    on_exit(fn -> :telemetry.detach(handler) end)
  end

  def forward_span([:kafka_ex, event, phase], _measurements, metadata, test) do
    send(test, {:pin_span, self(), System.unique_integer([:monotonic]), event, phase, metadata})
  end

  defp spans(host) do
    events = drain([])
    contexts = for {_, _, _, :start, %{host: ^host} = meta} <- events, do: meta.telemetry_span_context

    for {pid, seq, event, :start, meta} <- events, meta.telemetry_span_context in contexts do
      stop =
        Enum.find(events, fn {_, _, _, phase, stop_meta} ->
          phase == :stop and stop_meta.telemetry_span_context == meta.telemetry_span_context
        end)

      {stop_pid, stop_seq, stop_meta} =
        case stop do
          {stop_pid, stop_seq, _, :stop, stop_meta} -> {stop_pid, stop_seq, stop_meta}
          nil -> {nil, nil, nil}
        end

      %{event: event, pid: pid, from: seq, to: stop_seq, stop_pid: stop_pid, start: meta, stop: stop_meta}
    end
  end

  defp drain(acc) do
    receive do
      {:pin_span, pid, seq, event, phase, meta} -> drain([{pid, seq, event, phase, meta} | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  defp encloses?(outer, inner) do
    inner.pid == outer.pid and inner.to != nil and outer.to != nil and outer.from < inner.from and inner.to < outer.to
  end

  defp keys(metadata), do: metadata |> Map.delete(:telemetry_span_context) |> Map.keys() |> Enum.sort()
end
