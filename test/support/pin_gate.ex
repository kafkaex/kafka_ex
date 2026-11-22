defmodule KafkaEx.TestSupport.PinGate do
  @moduledoc """
  A `KafkaEx.TestSupport.FakeTransport` wrapper that can make a broker unreachable.

  The gate travels in `ssl_options` next to the fake (`ssl_options/2`). While it is closed every
  connect attempt returns `{:error, :econnrefused}`, as an unreachable broker does. Without a gate
  in `ssl_options` it is the plain fake.
  """

  @behaviour KafkaEx.Network.Transport

  alias KafkaEx.Network.Socket
  alias KafkaEx.TestSupport.FakeTransport

  @closed_until 1

  def new do
    gate = :atomics.new(1, signed: true)
    open(gate)
    gate
  end

  def ssl_options(gate, fake), do: [{:pin_gate, gate} | FakeTransport.ssl_options(fake)]

  def open(gate), do: :atomics.put(gate, @closed_until, System.monotonic_time(:millisecond))

  # close/1 is both the gate API and the Transport callback: a gate is an atomics ref, never a Socket.
  @impl KafkaEx.Network.Transport
  def close(%Socket{} = socket), do: FakeTransport.close(socket)
  def close(gate), do: :atomics.put(gate, @closed_until, :atomics.info(gate).max)

  def close_for(gate, ms), do: :atomics.put(gate, @closed_until, System.monotonic_time(:millisecond) + ms)

  @impl KafkaEx.Network.Transport
  def connect(address, socket_options, use_ssl, timeout) do
    case List.keytake(socket_options, :pin_gate, 0) do
      {{:pin_gate, gate}, rest} ->
        if closed?(gate),
          do: {:error, :econnrefused},
          else: FakeTransport.connect(address, rest, use_ssl, timeout)

      nil ->
        FakeTransport.connect(address, socket_options, use_ssl, timeout)
    end
  end

  @impl KafkaEx.Network.Transport
  defdelegate write(socket, data), to: FakeTransport

  @impl KafkaEx.Network.Transport
  defdelegate read(socket, timeout), to: FakeTransport

  @impl KafkaEx.Network.Transport
  defdelegate activate(socket), to: FakeTransport

  defp closed?(gate), do: System.monotonic_time(:millisecond) < :atomics.get(gate, @closed_until)
end
