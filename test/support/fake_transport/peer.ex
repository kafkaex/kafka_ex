defmodule KafkaEx.TestSupport.FakeTransport.Peer do
  @moduledoc false
  use GenServer

  @accept_timeout 5_000

  def start_link(args), do: GenServer.start_link(__MODULE__, args)

  @impl true
  def init(args), do: {:ok, Map.put(args, :socket, nil)}

  @impl true
  def handle_info(:accept, %{listener: listener} = state) do
    accepted = :gen_tcp.accept(listener, @accept_timeout)
    :gen_tcp.close(listener)

    with {:ok, tcp} <- accepted,
         {:ok, socket} <- upgrade(tcp, state.tls) do
      state = %{state | socket: socket}
      rearm(state)
      {:noreply, state}
    else
      {:error, _reason} -> stop(state)
    end
  end

  def handle_info({tag, socket, frame}, %{socket: socket} = state) when tag in [:tcp, :ssl] do
    action = GenServer.call(state.controller, {:request, state.conn_id, frame})

    case perform(action, state) do
      :closed ->
        stop(state)

      :open ->
        rearm(state)
        {:noreply, state}
    end
  end

  def handle_info({:action, action}, state) do
    case perform(action, state) do
      :closed -> stop(state)
      :open -> {:noreply, state}
    end
  end

  def handle_info(:close, state) do
    close(state)
    stop(state)
  end

  def handle_info({tag, socket}, %{socket: socket} = state) when tag in [:tcp_closed, :ssl_closed], do: stop(state)

  def handle_info({tag, socket, _reason}, %{socket: socket} = state) when tag in [:tcp_error, :ssl_error],
    do: stop(state)

  defp perform({:reply, iodata}, state) do
    _ = write(state, iodata)
    :open
  end

  defp perform(action, state) when action in [:close, :unscripted] do
    close(state)
    :closed
  end

  defp perform(_no_reply_or_hold, _state), do: :open

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
  defp close(%{tls: nil, socket: socket}), do: :gen_tcp.close(socket)
  defp close(%{socket: socket}), do: :ssl.close(socket)
end
