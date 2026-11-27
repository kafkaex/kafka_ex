defmodule KafkaEx.Client.FirstAvailableConnectTest do
  use KafkaEx.TestSupport.PinCase, async: false

  alias KafkaEx.Client.MetadataStore

  @moduletag pin_set: :invariant

  @ports [9092, 9093]

  setup do
    start_pinned(connection: [reconnect_window: 50, connect_timeout: 1_000])
  end

  # The shared reconnect backoff holds every broker's next connect back by `ms`.
  defp disconnect_all(%{client: client, fake: fake, host: host}, ms) do
    %{store: %{pid: store}, reconnect_backoff: backoff} = :sys.get_state(client)

    for port <- @ports do
      {:ok, pid} = MetadataStore.connection(store, {host, port}, 1_000)
      FakeTransport.close(fake, {host, port})
      assert {state, _data} = :sys.get_state(pid)
      refute state == :connected
      :ets.insert(backoff, {{host, port}, System.monotonic_time(:millisecond) + ms, 1})
    end
  end

  defp timed(fun) do
    started = System.monotonic_time(:millisecond)
    {result, _log} = with_log(fun)
    {result, System.monotonic_time(:millisecond) - started}
  end

  for {api, call, message} <- [
        {:find_coordinator, quote(do: API.find_coordinator(var!(client), "g")),
         Macro.escape({:find_coordinator, "g", []})},
        {:metadata, quote(do: API.metadata(var!(client))), Macro.escape({:metadata, nil, [], 1})},
        {:api_versions, quote(do: API.api_versions(var!(client))), Macro.escape({:api_versions, []})}
      ] do
    describe "#{api}" do
      test "waits for a candidate whose connection comes up within connect_timeout", %{client: client} = pinned do
        disconnect_all(pinned, 700)

        {result, elapsed} = timed(fn -> unquote(call) end)

        assert {:ok, _} = result
        assert elapsed >= 600
      end

      test "moves past a candidate that does not connect within connect_timeout", %{client: client} = pinned do
        %{host: host} = pinned
        disconnect_all(pinned, 30_000)
        %{reconnect_backoff: backoff} = :sys.get_state(client)
        :ets.insert(backoff, {{host, 9093}, System.monotonic_time(:millisecond), 1})

        {result, elapsed} = timed(fn -> unquote(call) end)

        assert {:ok, _} = result
        assert elapsed < 1_000 + 500 + 1_000
      end

      test "ends on :timeout when the request's deadline runs out first", %{client: client} = pinned do
        disconnect_all(pinned, 30_000)

        {result, elapsed} = timed(fn -> Client.request(client, unquote(message), 400) end)

        assert {:error, %{error: :timeout}} = result
        assert elapsed < 400 + 1_000
      end
    end
  end
end
