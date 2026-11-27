defmodule KafkaEx.TestSupport.StoreCase do
  @moduledoc """
  Starts a `KafkaEx.Client.Supervisor` with a `KafkaEx.Client.MetadataStore` on the fake transport,
  with the test process as the store's client.

  `start_store/2` takes the `%{fake:, host:}` from `PinCase.start_fake/2` (brokers on 9092 and
  9093 by default) and the options `:bootstrap` (ports), `:initial_topics`, `:interval`,
  `:allow_auto_topic_creation`, `:gate`, `:connection` (the `ConnectionStateMachine` options) and `:store`
  (the store's own options).
  """

  use ExUnit.CaseTemplate

  import ExUnit.Assertions
  import ExUnit.Callbacks, only: [start_supervised!: 1]

  alias KafkaEx.Client.ConnectionStateMachine
  alias KafkaEx.Client.MetadataStore
  alias KafkaEx.Client.Supervisor, as: ClientSupervisor
  alias KafkaEx.TestSupport.PinCase
  alias KafkaEx.TestSupport.PinGate

  @versions %{find_coordinator: 1..1}

  using do
    quote do
      import ExUnit.CaptureLog
      import KafkaEx.TestSupport.PinCase
      import KafkaEx.TestSupport.StoreCase

      alias KafkaEx.Client.ConnectionStateMachine
      alias KafkaEx.Client.MetadataStore
      alias KafkaEx.Client.MetadataStore.MetadataTable
      alias KafkaEx.Client.NodeSelector
      alias KafkaEx.Client.Supervisor, as: ClientSupervisor
      alias KafkaEx.TestSupport.FakeTransport
      alias KafkaEx.TestSupport.PinGate
    end
  end

  def versions, do: @versions

  def start_store(%{fake: fake, host: host}, opts \\ []) do
    PinCase.stub_transport()
    PinCase.with_connection(Keyword.get(opts, :connection, []))
    PinCase.with_store(Keyword.get(opts, :store, []))
    gate = Keyword.get_lazy(opts, :gate, &PinGate.new/0)

    table = &ConnectionStateMachine.reconnect_backoff_table/0
    backoff = Agent.get(start_supervised!(%{id: make_ref(), start: {Agent, :start_link, [table]}}), & &1)

    ssl_options = PinGate.ssl_options(gate, fake)

    args = %{
      client: self(),
      bootstrap: Enum.map(Keyword.get(opts, :bootstrap, [9092, 9093]), &{host, &1}),
      initial_topics: Keyword.get(opts, :initial_topics, ["pin"]),
      interval: Keyword.get(opts, :interval, 3_600_000),
      allow_auto_topic_creation: Keyword.get(opts, :allow_auto_topic_creation, true),
      connection: %{
        use_ssl: false,
        secrets: fn -> %{ssl_options: ssl_options, auth: nil} end,
        reconnect_backoff: backoff
      }
    }

    supervisor =
      start_supervised!(%{id: make_ref(), start: {ClientSupervisor, :start_link, [%{store: args}]}, type: :supervisor})

    %{
      supervisor: supervisor,
      store: ClientSupervisor.metadata_store(supervisor),
      reconnect_backoff: backoff,
      gate: gate
    }
  end

  def await!(store) do
    assert {:ok, ready} = MetadataStore.await_ready(store, 5_000)
    ready
  end

  def refresh(topics, allow \\ true), do: %{topics: topics, allow_auto_topic_creation: allow}

  def requested_topics(%{api_version: 0, frame: frame}) do
    <<_header::binary-size(8), client_id_size::16-signed, _client_id::binary-size(client_id_size), count::32-signed,
      rest::binary>> = frame

    {names, _rest} =
      Enum.map_reduce(1..count//1, rest, fn _, <<size::16-signed, name::binary-size(size), tail::binary>> ->
        {name, tail}
      end)

    MapSet.new(names)
  end

  def watch_store_events do
    test = self()
    handler = "store-events-#{System.unique_integer([:positive])}"

    :telemetry.attach_many(
      handler,
      [[:kafka_ex, :metadata_store, :degraded], [:kafka_ex, :metadata_store, :recovered]],
      fn event, measurements, metadata, _config -> send(test, {:store_event, event, measurements, metadata}) end,
      nil
    )

    ExUnit.Callbacks.on_exit(fn -> :telemetry.detach(handler) end)
  end
end
