defmodule KafkaEx.TestSupport.FakeTransport.Peer do
  @moduledoc false
  use GenServer

  @accept_timeout 5_000

  def start_link(args), do: GenServer.start_link(__MODULE__, args)

  @impl true
  def init(args), do: {:ok, Map.merge(args, %{socket: nil, muted?: false, stalled?: false, queue: []})}

  @impl true
  def handle_info(:accept, %{listener: listener, tls: :silent} = state) do
    accepted = :gen_tcp.accept(listener, @accept_timeout)
    :gen_tcp.close(listener)

    case accepted do
      {:ok, tcp} -> {:noreply, %{state | socket: tcp}}
      {:error, _reason} -> stop(state)
    end
  end

  def handle_info(:accept, %{listener: listener} = state) do
    accepted = :gen_tcp.accept(listener, @accept_timeout)
    :gen_tcp.close(listener)

    with {:ok, tcp} <- accepted,
         {:ok, socket} <- upgrade(tcp, state.tls) do
      read_next(%{state | socket: socket})
    else
      {:error, _reason} -> stop(state)
    end
  end

  def handle_info({tag, socket, frame}, %{socket: socket} = state) when tag in [:tcp, :ssl] do
    with {:noreply, state} <- drain(%{state | queue: state.queue ++ [frame]}), do: read_next(state)
  end

  def handle_info({:action, action}, state) do
    with {:noreply, state} <- perform(action, state), do: drain(state)
  end

  def handle_info(:close, state), do: perform(:close, state)

  def handle_info({tag, socket}, %{socket: socket} = state) when tag in [:tcp_closed, :ssl_closed], do: stop(state)

  def handle_info({tag, socket, _reason}, %{socket: socket} = state) when tag in [:tcp_error, :ssl_error],
    do: stop(state)

  def handle_info(_stray, state), do: {:noreply, state}

  defp drain(%{muted?: false, queue: [frame | queue]} = state) do
    action = GenServer.call(state.controller, {:request, state.conn_id, frame})
    with {:noreply, state} <- perform(action, %{state | queue: queue}), do: drain(state)
  end

  defp drain(state), do: {:noreply, state}

  defp perform({:reply, iodata}, state) do
    _ = write(state, iodata)
    {:noreply, %{state | muted?: false}}
  end

  defp perform(:acks0, state), do: {:noreply, state}

  defp perform(action, state) when action in [:hold, :no_reply], do: {:noreply, %{state | muted?: true}}

  defp perform(:stall, state), do: {:noreply, %{state | muted?: true, stalled?: true}}

  defp perform(action, state) when action in [:close, :unscripted, :unsupported_version] do
    close(state)
    stop(state)
  end

  defp read_next(%{stalled?: true} = state), do: {:noreply, state}

  defp read_next(state) do
    case rearm(state) do
      :ok -> {:noreply, state}
      {:error, _reason} -> stop(state)
    end
  end

  defp stop(state) do
    send(state.controller, {:peer_closed, state.conn_id})
    {:stop, :normal, state}
  end

  defp upgrade(tcp, nil), do: {:ok, tcp}
  defp upgrade(tcp, tls), do: :ssl.handshake(tcp, [:binary, packet: 4, active: false] ++ tls, @accept_timeout)

  defp rearm(%{tls: nil, socket: socket}), do: :inet.setopts(socket, active: :once)
  defp rearm(%{socket: socket}), do: :ssl.setopts(socket, active: :once)

  defp write(%{tls: nil, socket: socket}, iodata), do: :gen_tcp.send(socket, iodata)
  defp write(%{socket: socket}, iodata), do: :ssl.send(socket, iodata)

  defp close(%{socket: nil}), do: :ok
  defp close(%{tls: tls, socket: socket}) when tls in [nil, :silent], do: :gen_tcp.close(socket)
  defp close(%{socket: socket}), do: :ssl.close(socket)
end
