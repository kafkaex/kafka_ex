defmodule KafkaEx.Auth.SASL.Plain do
  @moduledoc """
  SASL/PLAIN mechanism for Kafka.

  Encodes/decodes SASL/PLAIN exchanges and drives the handshake/authenticate
  round-trips via the `KafkaEx.Auth.SASL` flow.

  ## Notes
    * **TLS required**: PLAIN must only be used over SSL/TLS.
    * Sends `<<0, username, 0, password>>` as the auth bytes.

  ## See also
    * `KafkaEx.Auth.SASL` – transport/flow
    * `KafkaEx.Auth.Config` – configuration source
  """

  @behaviour KafkaEx.Auth.Mechanism
  alias KafkaEx.Auth.Config

  @plain "PLAIN"

  @impl true
  def mechanism_name(%Config{}), do: @plain

  @impl true
  def authenticate(%Config{username: u, password: p}, send_fun) do
    msg = <<0, u::binary, 0, p::binary>>
    case send_fun.(msg) do
      {:ok, _} -> :ok
      error -> error
    end
  end
end
