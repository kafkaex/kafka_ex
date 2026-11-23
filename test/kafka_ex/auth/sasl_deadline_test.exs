defmodule KafkaEx.Auth.SASLDeadlineTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias KafkaEx.Auth.Config, as: AuthConfig
  alias KafkaEx.Auth.SASL
  alias KafkaEx.TestSupport.FakeTransport

  @sasl_versions %{17 => {0, 1}, 36 => {0, 1}}

  defp auth, do: AuthConfig.new(%{mechanism: :plain, username: "u", password: "p"})

  defp connect(fake) do
    {:ok, socket} =
      FakeTransport.connect(
        %{host: "a", port: 1, role: :metadata},
        FakeTransport.ssl_options(fake, [:binary, {:packet, 4}, {:active, false}]),
        false,
        1_000
      )

    socket
  end

  defp apis(fake), do: fake |> FakeTransport.requests() |> Enum.map(& &1.api)

  setup do
    %{fake: start_supervised!({FakeTransport, []})}
  end

  test "given ApiVersions, the handshake sends no ApiVersions request", %{fake: fake} do
    assert SASL.authenticate(connect(fake), auth(), api_versions: @sasl_versions) == :ok
    assert apis(fake) == [:sasl_handshake, :sasl_authenticate]
  end

  test "a broker that never answers SaslAuthenticate fails the handshake at the deadline", %{fake: fake} do
    FakeTransport.respond(fake, :sasl_authenticate, :no_reply)
    socket = connect(fake)
    deadline = System.monotonic_time(:millisecond) + 200

    task =
      Task.async(fn ->
        {result, _log} =
          with_log(fn -> SASL.authenticate(socket, auth(), api_versions: @sasl_versions, deadline: deadline) end)

        result
      end)

    assert_receive {ref, {:error, :timeout}} when ref == task.ref, 2_000
    assert System.monotonic_time(:millisecond) >= deadline
  end

  test "an expired deadline fails the handshake without waiting", %{fake: fake} do
    socket = connect(fake)
    deadline = System.monotonic_time(:millisecond) - 1

    capture_log(fn ->
      assert SASL.authenticate(socket, auth(), api_versions: @sasl_versions, deadline: deadline) == {:error, :timeout}
    end)

    assert apis(fake) == []
  end

  test "authenticate/2 still asks for ApiVersions once", %{fake: fake} do
    assert SASL.authenticate(connect(fake), auth()) == :ok
    assert apis(fake) == [:api_versions, :sasl_handshake, :sasl_authenticate]
  end
end
