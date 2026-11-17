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

  def metadata_reply(host, topics) do
    %{
      brokers: [%{node_id: 1, host: host, port: 9092}, %{node_id: 2, host: host, port: 9093}],
      topics:
        Enum.map(topics, fn {name, leaders} ->
          partitions =
            leaders
            |> Enum.with_index()
            |> Enum.map(fn {leader, index} ->
              %{partition_index: index, leader_id: leader, replica_nodes: [leader], isr_nodes: [leader]}
            end)

          %{name: name, partitions: partitions}
        end)
    }
  end

  def fetch_frame(%{api_version: 0, correlation_id: corr}, _preferred) do
    <<corr::32-signed, 1::32-signed, byte_size(@topic)::16-signed, @topic::binary, 1::32-signed, 0::32-signed,
      0::16-signed, 10::64-signed, 0::32-signed>>
  end

  # The fake's encoder cannot write the nullable assignment struct.
  def consumer_group_heartbeat_frame(%{correlation_id: corr}, member_id, epoch) do
    <<corr::32-signed, 0, 0::32-signed, 0::16-signed, 0, byte_size(member_id) + 1, member_id::binary, epoch::32-signed,
      1000::32-signed, 255, 0>>
  end

  def member_epoch(%{frame: frame}) do
    <<_api::16, _version::16, _corr::32, client_id_size::16-signed, _client_id::binary-size(client_id_size), 0,
      rest::binary>> = frame

    {_group, rest} = compact_string(rest)
    {_member, <<epoch::32-signed, _::binary>>} = compact_string(rest)
    epoch
  end

  defp compact_string(<<length_plus_one, rest::binary>>) when length_plus_one < 128 do
    size = length_plus_one - 1
    <<value::binary-size(size), rest::binary>> = rest
    {value, rest}
  end

  def watch_close(host) do
    test = self()
    handler = "pin-close-#{host}"

    :telemetry.attach(
      handler,
      [:kafka_ex, :connection, :close],
      fn _event, _measurements, metadata, _config ->
        if metadata.host == host, do: send(test, {:pin_closed, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)
  end

  def with_request_timeout(ms) do
    previous = Application.get_env(:kafka_ex, :request_timeout)
    Application.put_env(:kafka_ex, :request_timeout, ms)
    on_exit(fn -> Application.put_env(:kafka_ex, :request_timeout, previous) end)
  end
end
