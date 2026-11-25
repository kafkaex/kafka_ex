defmodule KafkaEx.TestSupport.PinCase do
  @moduledoc """
  Shared setup for the behaviour pins: a client started on the fake transport, plus helpers that
  read the fake's request log by API and node.

  `non_handshake/1` leaves out the handshake (ApiVersions, SASL) because callers never ask for it;
  `requests/2` does not filter it.
  """

  use ExUnit.CaseTemplate

  import ExUnit.Assertions
  import ExUnit.Callbacks, only: [start_supervised!: 1, on_exit: 1]

  alias KafkaEx.Client
  alias KafkaEx.Client.ConnectionStateMachine
  alias KafkaEx.Client.Supervisor, as: ClientSupervisor
  alias KafkaEx.Network.NetworkClient
  alias KafkaEx.TestSupport.FakeTransport
  alias KafkaEx.TestSupport.PinGate

  @topic "pin"
  @handshake [:api_versions, :sasl_handshake, :sasl_authenticate]
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
      import KafkaEx.TestHelpers, only: [forward_telemetry: 2, oauthbearer: 0, oauthbearer: 1]
      import KafkaEx.TestSupport.PinCase

      alias KafkaEx.API
      alias KafkaEx.Client
      alias KafkaEx.TestSupport.FakeTransport
      alias KafkaEx.TestSupport.PinGate

      @topic "pin"
    end
  end

  def stub_transport(transport \\ PinGate) do
    Mimic.set_mimic_global()
    Mimic.stub(NetworkClient, :connect, &transport.connect/4)
    Mimic.stub(NetworkClient, :write, &transport.write/2)
    Mimic.stub(NetworkClient, :read, &transport.read/2)
    Mimic.stub(NetworkClient, :activate, &transport.activate/1)
    Mimic.stub(NetworkClient, :close, &transport.close/1)
  end

  def start_pinned(opts \\ []) do
    with_connection(Keyword.get(opts, :connection, []))
    with_store(Keyword.get(opts, :store, []))
    versions = @versions |> Map.merge(Keyword.get(opts, :api_versions, %{})) |> Map.reject(&is_nil(elem(&1, 1)))
    topics = Keyword.get(opts, :topics, %{@topic => [1, 2]})
    fake_opts = [topics: topics, api_versions: versions] ++ Keyword.get(opts, :fake, [])
    %{fake: fake, host: host} = start_fake([{1, 9092}, {2, 9093}], fake_opts)

    :ok = FakeTransport.respond(fake, :find_coordinator, %{node_id: 2, host: host, port: 9093, error_message: nil})
    Keyword.get(opts, :before_boot, fn _fake -> :ok end).(fake)
    gate = PinGate.new()

    client_opts =
      [
        uris: Enum.map(Keyword.get(opts, :bootstrap, [9092]), &{host, &1}),
        consumer_group: :no_consumer_group,
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

  def start_fake(brokers, opts \\ []) do
    stub_transport()
    host = "pin-#{System.unique_integer([:positive])}"
    brokers = Enum.map(brokers, fn {node_id, port} -> {node_id, host, port} end)
    fake = start_supervised!(Supervisor.child_spec({FakeTransport, [brokers: brokers] ++ opts}, id: make_ref()))
    %{fake: fake, host: host}
  end

  def requests(fake, api), do: fake |> FakeTransport.requests() |> Enum.filter(&(&1.api == api))
  def count(fake, api), do: fake |> requests(api) |> length()
  def count(fake, api, node_id), do: fake |> requests(api) |> Enum.count(&(&1.node_id == node_id))
  def nodes(fake, api), do: fake |> requests(api) |> Enum.map(& &1.node_id)

  def non_handshake(fake), do: fake |> FakeTransport.requests() |> Enum.reject(&handshake?/1)
  def handshake?(%{api: api}), do: api in @handshake

  def apis_after(fake, mark), do: fake |> FakeTransport.requests() |> Enum.drop(mark) |> Enum.map(& &1.api)

  # A failed send never reaches the fake; the log line from ConnectionStateMachine is the only trace of the attempt.
  def failed_sends(log, host, reason), do: count_failures(log, "Sending data to", host, reason)
  def failed_async_sends(log, host, reason), do: count_failures(log, "Asynchronously sending data to", host, reason)

  defp count_failures(log, verb, host, reason) do
    line =
      ~r/#{verb} broker #{Regex.escape(inspect(host))}:\d+ failed with #{Regex.escape(inspect(reason))}\n/

    length(Regex.scan(line, log))
  end

  def produce_reply(topic \\ @topic) do
    %{
      responses: [
        %{
          topic: topic,
          partition_responses: [%{partition: 0, error_code: 0, base_offset: 7, log_append_time: -1}]
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

  def unknown_topic_reply(host, name) do
    %{metadata_reply(host, %{}) | topics: [%{error_code: 3, name: name, is_internal: false, partitions: []}]}
  end

  def leaderless_reply(host, name) do
    %{metadata_reply(host, %{}) | topics: [%{error_code: 5, name: name, is_internal: false, partitions: []}]}
  end

  def fetch_frame(%{api_version: 0, correlation_id: corr}, _preferred) do
    <<corr::32-signed, 1::32-signed, byte_size(@topic)::16-signed, @topic::binary, 1::32-signed, 0::32-signed,
      0::16-signed, 10::64-signed, 0::32-signed>>
  end

  def fetch_frame(%{api_version: 11, correlation_id: corr}, preferred) do
    <<corr::32-signed, 0::32-signed, 0::16-signed, 0::32-signed, 1::32-signed, byte_size(@topic)::16-signed,
      @topic::binary, 1::32-signed, 0::32-signed, 0::16-signed, 10::64-signed, 10::64-signed, 0::64-signed,
      0::32-signed, preferred::32-signed, 0::32-signed>>
  end

  # The fake's encoder writes a nil array as an empty one.
  def null_topics_metadata_frame(%{api_version: 12, correlation_id: corr}) do
    <<corr::32-signed, 0, 0::32-signed, 1, 0, -1::32-signed, 0, 0>>
  end

  # The fake's encoder cannot write the nullable assignment struct.
  def consumer_group_heartbeat_frame(%{correlation_id: corr}, member_id, epoch) when byte_size(member_id) < 127 do
    <<corr::32-signed, 0, 0::32-signed, 0::16-signed, 0, byte_size(member_id) + 1, member_id::binary, epoch::32-signed,
      1000::32-signed, 255, 0>>
  end

  def member_epoch(request) do
    {_member, <<epoch::32-signed, _::binary>>} = heartbeat_member(request)
    epoch
  end

  def member_id(request) do
    {member, _rest} = heartbeat_member(request)
    member
  end

  defp heartbeat_member(%{frame: frame}) do
    <<_api::16, _version::16, _corr::32, client_id_size::16-signed, _client_id::binary-size(client_id_size), 0,
      rest::binary>> = frame

    {_group, rest} = compact_string(rest)
    compact_string(rest)
  end

  defp compact_string(<<length_plus_one, rest::binary>>) when length_plus_one < 128 do
    size = length_plus_one - 1
    <<value::binary-size(^size), rest::binary>> = rest
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

  def drop_connection(fake, host) do
    FakeTransport.close(fake, {host, 9092})
    assert_receive {:pin_closed, %{port: 9092}}, 5_000
  end

  def fail_connection_start, do: with_env(:connection, :invalid)

  def with_request_timeout(ms), do: with_env(:request_timeout, ms)

  def with_connection(opts) do
    {connect_timeout, settings} = Keyword.pop(opts, :connect_timeout)
    if connect_timeout, do: with_env(:connect_timeout, connect_timeout)
    with_env(:connection, settings)
  end

  def with_store(opts), do: with_env(:metadata_store, opts)

  def with_env(key, value) do
    previous = Application.fetch_env(:kafka_ex, key)
    Application.put_env(:kafka_ex, key, value)

    on_exit(fn ->
      case previous do
        {:ok, previous} -> Application.put_env(:kafka_ex, key, previous)
        :error -> Application.delete_env(:kafka_ex, key)
      end
    end)
  end

  # The connection supervisor runs as Supervisor.Default, as ExUnit <= 1.16's test supervisor does: diff two calls.
  def supervisor_processes do
    for pid <- Process.list(),
        {:dictionary, dictionary} <- [Process.info(pid, :dictionary)],
        self() in Keyword.get(dictionary, :"$ancestors", []),
        Keyword.get(dictionary, :"$initial_call") in [
          {ConnectionStateMachine, :init, 1},
          {:supervisor, ClientSupervisor, 1},
          {:supervisor, Supervisor.Default, 1}
        ] do
      pid
    end
  end
end
