defmodule KafkaEx.TestSupport.ConnectionCase do
  @moduledoc """
  Starts a `KafkaEx.Client.ConnectionStateMachine` on the fake transport and builds its dispatches.

  `start_connection/5` takes the `ConnectionStateMachine` options plus `:reconnect_backoff`, `:auth`,
  `:use_ssl`, `:ssl_options`, `:gate` (a `PinGate`) and `:secrets`.
  """

  use ExUnit.CaseTemplate

  import ExUnit.Assertions
  import ExUnit.Callbacks, only: [start_supervised!: 1]
  import ExUnit.CaptureLog
  import KafkaEx.TestHelpers, only: [forward_telemetry: 2]

  alias KafkaEx.Client.ConnectionStateMachine
  alias KafkaEx.Client.ConnectionStateMachine.ReconnectBackoff
  alias KafkaEx.Client.RequestBuilder
  alias KafkaEx.Client.State
  alias KafkaEx.Protocol.KayrockProtocol
  alias KafkaEx.TestSupport.FakeTransport
  alias KafkaEx.TestSupport.PinCase
  alias KafkaEx.TestSupport.PinGate

  @port 9092
  @call_timeout 10_000
  @case_options [:reconnect_backoff, :auth, :use_ssl, :ssl_options, :gate, :secrets]
  @versions %{
    produce: 3..3,
    fetch: 0..0,
    heartbeat: 0..0,
    offset_commit: 2..2,
    list_groups: 0..0
  }

  using do
    quote do
      import ExUnit.CaptureLog
      import KafkaEx.TestHelpers, only: [forward_telemetry: 2, oauthbearer: 0, oauthbearer: 1]
      import KafkaEx.TestSupport.ConnectionCase
      import KafkaEx.TestSupport.PinCase, only: [requests: 2]

      alias KafkaEx.Client.ConnectionStateMachine
      alias KafkaEx.Client.ConnectionStateMachine.ReconnectBackoff
      alias KafkaEx.TestSupport.FakeTransport
      alias KafkaEx.TestSupport.PinGate
    end
  end

  def port, do: @port

  def start_fake(opts \\ []) do
    PinCase.stub_transport()
    host = "conn-#{System.unique_integer([:positive])}"
    versions = Map.merge(@versions, Keyword.get(opts, :api_versions, %{}))
    fake_opts = [brokers: [{1, host, @port}], api_versions: versions] ++ Keyword.drop(opts, [:api_versions])
    fake = start_supervised!(Supervisor.child_spec({FakeTransport, fake_opts}, id: make_ref()))
    %{fake: fake, host: host}
  end

  def start_connection(fake, host, port, role, opts \\ []) do
    PinCase.with_connection(Keyword.drop(opts, @case_options))
    backoff = Keyword.get_lazy(opts, :reconnect_backoff, &ReconnectBackoff.new/0)

    pid =
      start_supervised!(%{
        ConnectionStateMachine.child_spec(args(fake, host, port, role, backoff, opts))
        | id: make_ref()
      })

    %{conn: pid, reconnect_backoff: backoff}
  end

  def start_connected(fake, host, role, opts \\ []) do
    %{conn: conn} = started = start_connection(fake, host, @port, role, opts)
    {:ok, _versions} = ConnectionStateMachine.connect(conn, 5_000)
    started
  end

  def args(fake, host, port, role, backoff, opts) do
    ssl_options = ssl_options(fake, opts)
    auth = opts[:auth]

    %{
      client: self(),
      host: host,
      port: port,
      role: role,
      use_ssl: Keyword.get(opts, :use_ssl, false),
      secrets: Keyword.get(opts, :secrets, fn -> %{ssl_options: ssl_options, auth: auth} end),
      reconnect_backoff: backoff
    }
  end

  defp ssl_options(fake, opts) do
    extra = Keyword.get(opts, :ssl_options, [])

    case opts[:gate] do
      nil -> FakeTransport.ssl_options(fake, extra)
      gate -> PinGate.ssl_options(gate, fake) ++ extra
    end
  end

  def dispatch(conn, request, overrides \\ []) do
    dispatch = build_dispatch(request, overrides)

    with :ok <- ConnectionStateMachine.await(conn, dispatch, @call_timeout) do
      ConnectionStateMachine.write(conn, dispatch, @call_timeout)
    end
  end

  def build_dispatch(request, overrides \\ []) do
    Map.merge(
      %{
        request: request,
        info: KayrockProtocol.request_info(request),
        node_id: nil,
        reply?: true,
        timeout: 1_000,
        long_poll: nil,
        wait?: true,
        deadline: now() + 10_000
      },
      Map.new(overrides)
    )
  end

  def async_dispatch(conn, request, overrides \\ []) do
    ref = make_ref()
    test = self()
    spawn(fn -> send(test, {ref, dispatch(conn, request, overrides)}) end)
    ref
  end

  def now, do: System.monotonic_time(:millisecond)

  def correlate_from(conn, id) do
    :sys.replace_state(conn, fn {state, data} -> {state, %{data | next_id: id}} end)
  end

  def request(api, opts \\ []), do: build(api, opts, Map.new(@versions, fn {api, range} -> {key(api), range} end))

  defp key(api), do: KafkaEx.Protocol.KayrockProtocol.api_key(api)

  defp build(api, opts, versions) do
    state = %State{api_versions: Map.new(versions, fn {key, first..last//_} -> {key, {first, last}} end)}
    {:ok, request} = RequestBuilder.build(api, Keyword.merge(defaults(api), opts), state)
    %{request | client_id: "conn-test"}
  end

  defp defaults(:produce), do: [topic: "t", partition: 0, messages: [%{value: "v"}]]
  defp defaults(:fetch), do: [topic: "t", partition: 0, offset: 0]
  defp defaults(:heartbeat), do: [group_id: "g", member_id: "m", generation_id: 1]
  defp defaults(:offset_commit), do: [group_id: "g", topics: [{"t", [%{partition_num: 0, offset: 1}]}]]
  defp defaults(:list_groups), do: []

  def frame(payload), do: fn request -> {:frame, [<<request.correlation_id::32-signed>>, payload]} end

  def payload(<<_correlation_id::32-signed, payload::binary>>), do: payload

  def watch_close(host) do
    forward_telemetry([[:kafka_ex, :connection, :close]], fn _event, _measurements, metadata ->
      if metadata.host == host, do: {:closed, metadata}
    end)
  end

  def watch_attempts(host) do
    forward_telemetry([[:kafka_ex, :connection, :start]], fn _event, _measurements, metadata ->
      if metadata.host == host, do: {:attempt, self(), now()}
    end)
  end

  def inject_tcp_error(conn) do
    {:connected, %{socket: %{socket: raw}}} = :sys.get_state(conn)

    {_, log} =
      with_log([level: :debug], fn ->
        send(conn, {:tcp_error, raw, :econnreset})
        :sys.get_state(conn)
      end)

    log
  end

  def connection_state(conn) do
    {state, _data} = :sys.get_state(conn)
    state
  end

  def await_in_flight(conn, count) do
    await_data(conn, &(map_size(&1.in_flight) >= count), "#{count} requests never got in flight")
  end

  def await_waiting(conn, count) do
    await_data(conn, &(length(&1.waiting) >= count), "#{count} callers never queued")
  end

  defp await_data(conn, ready?, failure, deadline \\ now() + 2_000) do
    {_state, data} = :sys.get_state(conn)

    cond do
      ready?.(data) -> :ok
      now() >= deadline -> flunk(failure)
      true -> await_data(conn, ready?, failure, deadline)
    end
  end
end
