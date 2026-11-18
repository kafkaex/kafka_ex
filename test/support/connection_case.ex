defmodule KafkaEx.TestSupport.ConnectionCase do
  @moduledoc """
  Starts a `KafkaEx.Client.Connection` on the fake transport and builds its dispatches.

  `start_connection/5` takes the `Connection` options plus `:pacing`, `:auth`, `:use_ssl`,
  `:ssl_options`, `:gate` (a `PinGate`, which also selects it as the transport) and `:secrets`.
  """

  use ExUnit.CaseTemplate

  import ExUnit.Assertions
  import ExUnit.Callbacks, only: [start_supervised!: 1, on_exit: 1]

  alias KafkaEx.Client.Connection
  alias KafkaEx.Client.Connection.Pacing
  alias KafkaEx.Client.RequestBuilder
  alias KafkaEx.Client.State
  alias KafkaEx.Protocol.KayrockProtocol
  alias KafkaEx.TestSupport.FakeTransport
  alias KafkaEx.TestSupport.PinGate

  @port 9092
  @call_timeout 10_000
  @case_options [:pacing, :auth, :use_ssl, :ssl_options, :gate, :secrets]
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
      import KafkaEx.TestSupport.ConnectionCase

      alias KafkaEx.Client.Connection
      alias KafkaEx.Client.Connection.Pacing
      alias KafkaEx.TestSupport.FakeTransport
      alias KafkaEx.TestSupport.PinGate
    end
  end

  def port, do: @port

  def start_fake(opts \\ []) do
    host = "conn-#{System.unique_integer([:positive])}"
    versions = Map.merge(@versions, Keyword.get(opts, :api_versions, %{}))
    fake_opts = [brokers: [{1, host, @port}], api_versions: versions] ++ Keyword.drop(opts, [:api_versions])
    fake = start_supervised!(Supervisor.child_spec({FakeTransport, fake_opts}, id: make_ref()))
    %{fake: fake, host: host}
  end

  def start_connection(fake, host, port, role, opts \\ []) do
    pacing = Keyword.get_lazy(opts, :pacing, &Pacing.new/0)
    pid = start_supervised!(%{Connection.child_spec(args(fake, host, port, role, pacing, opts)) | id: make_ref()})
    %{conn: pid, pacing: pacing}
  end

  def args(fake, host, port, role, pacing, opts) do
    {transport, ssl_options} = transport(fake, opts)
    auth = opts[:auth]

    %{
      client: self(),
      host: host,
      port: port,
      role: role,
      use_ssl: Keyword.get(opts, :use_ssl, false),
      transport: transport,
      secrets: Keyword.get(opts, :secrets, fn -> %{ssl_options: ssl_options, auth: auth} end),
      pacing: pacing,
      opts: Keyword.drop(opts, @case_options)
    }
  end

  defp transport(fake, opts) do
    extra = Keyword.get(opts, :ssl_options, [])

    case opts[:gate] do
      nil -> {FakeTransport, FakeTransport.ssl_options(fake, extra)}
      gate -> {PinGate, PinGate.ssl_options(gate, fake) ++ extra}
    end
  end

  def dispatch(conn, request, overrides \\ []) do
    dispatch = build_dispatch(request, overrides)

    with :ok <- Connection.await(conn, dispatch, @call_timeout),
         do: Connection.write(conn, dispatch, @call_timeout)
  end

  def build_dispatch(request, overrides \\ []) do
    Map.merge(
      %{
        request: request,
        info: KayrockProtocol.request_info(request),
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

  def request(api, opts \\ []), do: build(api, opts, Map.new(@versions, fn {api, range} -> {key(api), range} end))

  defp key(api), do: KafkaEx.Protocol.KayrockProtocol.api_key(api)

  defp build(api, opts, versions) do
    state = %State{api_versions: Map.new(versions, fn {key, first..last//_} -> {key, {first, last}} end)}
    {:ok, request} = builder(api).(Keyword.merge(defaults(api), opts), state)
    %{request | client_id: "conn-test"}
  end

  defp builder(:produce), do: &RequestBuilder.produce_request/2
  defp builder(:fetch), do: &RequestBuilder.fetch_request/2
  defp builder(:heartbeat), do: &RequestBuilder.heartbeat_request/2
  defp builder(:offset_commit), do: &RequestBuilder.offset_commit_request/2
  defp builder(:list_groups), do: &RequestBuilder.list_groups_request/2

  defp defaults(:produce), do: [topic: "t", partition: 0, messages: [%{value: "v"}]]
  defp defaults(:fetch), do: [topic: "t", partition: 0, offset: 0]
  defp defaults(:heartbeat), do: [group_id: "g", member_id: "m", generation_id: 1]
  defp defaults(:offset_commit), do: [group_id: "g", topics: [{"t", [%{partition_num: 0, offset: 1}]}]]
  defp defaults(:list_groups), do: []

  def frame(payload), do: fn request -> {:frame, [<<request.correlation_id::32-signed>>, payload]} end

  def payload(<<_correlation_id::32-signed, payload::binary>>), do: payload

  def watch_close(host) do
    test = self()
    handler = "conn-close-#{host}-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      handler,
      [:kafka_ex, :connection, :close],
      fn _event, _measurements, metadata, _config ->
        if metadata.host == host, do: send(test, {:closed, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)
  end

  def watch_attempts(host) do
    test = self()
    handler = "conn-attempts-#{host}-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      handler,
      [:kafka_ex, :connection, :start],
      fn _event, _measurements, metadata, _config ->
        if metadata.host == host, do: send(test, {:attempt, self(), System.monotonic_time(:millisecond)})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)
  end

  def connection_state(conn) do
    {state, _data} = :sys.get_state(conn)
    state
  end

  def await_in_flight(conn, count),
    do: await_data(conn, &(map_size(&1.in_flight.entries) >= count), "#{count} requests never got in flight")

  def await_waiting(conn, count),
    do: await_data(conn, &(:queue.len(&1.waiting) >= count), "#{count} callers never queued")

  defp await_data(conn, ready?, failure, deadline \\ now() + 2_000) do
    {_state, data} = :sys.get_state(conn)

    cond do
      ready?.(data) -> :ok
      now() >= deadline -> flunk(failure)
      true -> await_data(conn, ready?, failure, deadline)
    end
  end

  def requests(fake, api), do: fake |> FakeTransport.requests() |> Enum.filter(&(&1.api == api))
end
