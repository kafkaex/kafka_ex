defmodule KafkaEx.TestSupport.PinGate do
  @moduledoc """
  A `KafkaEx.TestSupport.FakeTransport` wrapper that can make a broker unreachable.

  The gate travels in `ssl_options` next to the fake (`ssl_options/2`). While it is closed every
  connect attempt returns no socket, as an unreachable broker does; `raise_on_connect/1` makes the
  next connect attempts raise instead. Without a gate in `ssl_options` it is the plain fake.
  """

  @behaviour KafkaEx.Network.Behaviour

  alias KafkaEx.TestSupport.FakeTransport

  @closed_until 1
  @raising 2

  def new do
    gate = :atomics.new(2, signed: true)
    open(gate)
    gate
  end

  def ssl_options(gate, fake), do: [{:pin_gate, gate} | FakeTransport.ssl_options(fake)]

  def open(gate) do
    :atomics.put(gate, @raising, 0)
    :atomics.put(gate, @closed_until, System.monotonic_time(:millisecond))
  end

  def close(gate), do: :atomics.put(gate, @closed_until, :atomics.info(gate).max)
  def close_for(gate, ms), do: :atomics.put(gate, @closed_until, System.monotonic_time(:millisecond) + ms)
  def raise_on_connect(gate), do: :atomics.put(gate, @raising, 1)

  @impl true
  def create_socket(host, port, ssl_options \\ [], use_ssl \\ false, auth \\ nil) do
    case List.keytake(ssl_options, :pin_gate, 0) do
      {{:pin_gate, gate}, rest} -> gated_connect(gate, host, port, rest, use_ssl, auth)
      nil -> FakeTransport.create_socket(host, port, ssl_options, use_ssl, auth)
    end
  end

  @impl true
  defdelegate close_socket(socket), to: FakeTransport

  @impl true
  defdelegate close_socket(broker, socket, reason), to: FakeTransport

  @impl true
  defdelegate send_sync_request(broker, data, timeout), to: FakeTransport

  @impl true
  defdelegate send_async_request(broker, data), to: FakeTransport

  @impl true
  defdelegate format_host(host), to: FakeTransport

  defp gated_connect(gate, host, port, ssl_options, use_ssl, auth) do
    cond do
      :atomics.get(gate, @raising) == 1 -> raise "connect fault injected by the test"
      System.monotonic_time(:millisecond) < :atomics.get(gate, @closed_until) -> nil
      true -> FakeTransport.create_socket(host, port, ssl_options, use_ssl, auth)
    end
  end
end
