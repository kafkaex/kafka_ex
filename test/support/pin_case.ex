defmodule KafkaEx.TestSupport.PinCase do
  @moduledoc """
  Shared setup for the behaviour pins: a client started on the fake transport, plus helpers that
  read the fake's request log by API and node, with the handshake (ApiVersions, SASL) left out.
  """

  use ExUnit.CaseTemplate

  import ExUnit.Callbacks, only: [start_supervised!: 1, on_exit: 1]

  alias KafkaEx.Client
  alias KafkaEx.TestSupport.FakeTransport
  alias KafkaEx.TestSupport.PinGate

  @topic "pin"
  @versions %{
    produce: 3..3,
    fetch: 0..0,
    list_offsets: 1..1,
    find_coordinator: 1..1,
    join_group: 4..4,
    sync_group: 0..0,
    heartbeat: 0..0,
    leave_group: 0..0,
    offset_commit: 2..2,
    consumer_group_heartbeat: 0..0
  }

  using do
    quote do
      import ExUnit.CaptureLog
      import KafkaEx.TestSupport.PinCase

      alias KafkaEx.API
      alias KafkaEx.Client
      alias KafkaEx.TestSupport.FakeTransport
      alias KafkaEx.TestSupport.PinGate

      @topic "pin"
    end
  end

  def start_pinned(opts \\ []) do
    host = "pin-#{System.unique_integer([:positive])}"
    versions = Map.merge(@versions, Keyword.get(opts, :api_versions, %{}))
    topics = Keyword.get(opts, :topics, %{@topic => [1, 2]})

    fake =
      start_supervised!(
        {FakeTransport, brokers: [{1, host, 9092}, {2, host, 9093}], topics: topics, api_versions: versions}
      )

    :ok = FakeTransport.respond(fake, :find_coordinator, %{node_id: 2, host: host, port: 9093, error_message: nil})
    Keyword.get(opts, :before_boot, fn _fake -> :ok end).(fake)
    gate = PinGate.new()

    client_opts =
      [
        uris: Enum.map(Keyword.get(opts, :bootstrap, [9092]), &{host, &1}),
        consumer_group: :no_consumer_group,
        transport: PinGate,
        ssl_options: PinGate.ssl_options(gate, fake),
        metadata_update_interval: 3_600_000
      ]
      |> Keyword.merge(Keyword.get(opts, :client, []))

    client =
      start_supervised!(%{
        id: make_ref(),
        start: {Client, :start_link, [client_opts, Keyword.get(opts, :name, :no_name)]},
        restart: :temporary
      })

    %{fake: fake, client: client, host: host, gate: gate}
  end

  def requests(fake, api), do: fake |> FakeTransport.requests() |> Enum.filter(&(&1.api == api))
  def count(fake, api), do: fake |> requests(api) |> length()
  def nodes(fake, api), do: fake |> requests(api) |> Enum.map(& &1.node_id)

  def produce_reply(base_offset \\ 7) do
    %{
      responses: [
        %{
          topic: @topic,
          partition_responses: [%{partition: 0, error_code: 0, base_offset: base_offset, log_append_time: -1}]
        }
      ]
    }
  end

  def with_request_timeout(ms) do
    previous = Application.get_env(:kafka_ex, :request_timeout)
    Application.put_env(:kafka_ex, :request_timeout, ms)
    on_exit(fn -> Application.put_env(:kafka_ex, :request_timeout, previous) end)
  end
end
