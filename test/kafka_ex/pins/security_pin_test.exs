defmodule KafkaEx.Pins.SecurityPinTest do
  use KafkaEx.TestSupport.PinCase, async: false

  alias KafkaEx.Auth.Config, as: AuthConfig

  @moduletag pin_set: :invariant

  @sasl_authentication_failed 58

  test "no request is written before SASL succeeds, and none to a broker whose SASL fails" do
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
    first_auth = Enum.find_index(log, &(&1.api == :sasl_authenticate and &1.node_id == 1))
    first_request = Enum.find_index(log, &(&1.api not in [:api_versions, :sasl_handshake, :sasl_authenticate]))

    assert first_auth < first_request
    assert fake |> non_handshake() |> Enum.filter(&(&1.node_id == 2)) == []
    assert count(fake, :sasl_authenticate, 2) >= 1
  end
end
