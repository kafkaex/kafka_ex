defmodule KafkaEx.Pins.SecurityPinTest do
  use KafkaEx.TestSupport.PinCase, async: false

  alias KafkaEx.Auth.Config, as: AuthConfig

  @moduletag pin_set: :invariant

  @sasl_authentication_failed 58

  test "no request is written on a connection before its SASL authenticate, and none to a broker whose SASL fails" do
    with_request_timeout(2_000)

    auth =
      AuthConfig.new(%{mechanism: :oauthbearer, mechanism_opts: %{token_provider: fn -> {:ok, "t"} end}})

    {pinned, _log} =
      with_log(fn ->
        start_pinned(
          bootstrap: [9092, 9093],
          client: [auth: auth],
          before_boot: fn fake ->
            FakeTransport.respond(fake, :sasl_authenticate, fn request ->
              if request.node_id == 2,
                do: %{error_code: @sasl_authentication_failed, error_message: "denied", auth_bytes: ""},
                else: %{error_message: nil, auth_bytes: ""}
            end)
          end
        )
      end)

    %{fake: fake, client: client} = pinned
    FakeTransport.respond(fake, :produce, produce_reply())

    {:ok, _} = API.produce(client, @topic, 0, [%{value: "v"}])
    {{:error, _}, _log} = with_log(fn -> API.produce(client, @topic, 1, [%{value: "v"}]) end)

    log = FakeTransport.requests(fake)

    for conn <- log |> Enum.filter(&(&1.node_id == 1 and not handshake?(&1))) |> Enum.map(& &1.conn) |> Enum.uniq() do
      on_conn = Enum.filter(log, &(&1.conn == conn))
      first_auth = Enum.find_index(on_conn, &match?(%{api: :sasl_authenticate, action: :reply}, &1))

      assert is_integer(first_auth)
      assert on_conn |> Enum.take(first_auth) |> Enum.all?(&handshake?/1)
    end

    assert fake |> non_handshake() |> Enum.filter(&(&1.node_id == 1)) != []
    assert fake |> non_handshake() |> Enum.filter(&(&1.node_id == 2)) == []
    assert count(fake, :sasl_authenticate, 2) >= 1
  end

  test "PLAIN without TLS is refused before any request" do
    Process.flag(:trap_exit, true)
    host = "pin-#{System.unique_integer([:positive])}"
    fake = start_supervised!(Supervisor.child_spec({FakeTransport, brokers: [{1, host, 9092}]}, id: make_ref()))
    auth = AuthConfig.new(%{mechanism: :plain, username: "u", password: "p"})

    opts = [
      uris: [{host, 9092}],
      consumer_group: :no_consumer_group,
      transport: FakeTransport,
      ssl_options: FakeTransport.ssl_options(fake),
      use_ssl: false,
      auth: auth
    ]

    {result, _log} = with_log(fn -> Client.start_link(opts, :no_name) end)

    assert {:error, _reason} = result
    assert non_handshake(fake) == []
  end
end
