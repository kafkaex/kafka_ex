defmodule KafkaEx.TestSupport.SSLServer do
  def start do
    {:ok, listen_socket} =
      :ssl.listen(0, [
        :binary,
        {:verify, :verify_peer},
        {:active, false},
        {:packet, 0},
        {:cacertfile, ~c"test/fixtures/server.pem"},
        {:certfile, ~c"test/fixtures/server.crt"},
        {:keyfile, ~c"test/fixtures/server.key"}
      ])

    {:ok, {_addr, port}} = :ssl.sockname(listen_socket)

    {spawn_link(fn -> listen(listen_socket) end), port}
  end

  defp listen(socket) do
    case :ssl.transport_accept(socket) do
      {:ok, conn} ->
        {:ok, _socket} = :ssl.handshake(conn)
        pid = spawn_link(fn -> recv(conn) end)
        :ssl.controlling_process(socket, pid)

      _ ->
        :ok
    end

    listen(socket)
  end

  defp recv(conn) do
    case :ssl.recv(conn, 0) do
      {:ok, data} ->
        :ok = :ssl.send(conn, data)

      {:error, :closed} ->
        :ok
    end
  end
end
