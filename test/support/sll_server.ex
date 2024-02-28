defmodule KafkaEx.TestSupport.SSLServer do
  def start(port) do
    {:ok, listen_socket} =
      :ssl.listen(port, [
        :binary,
        {:verify, :verify_peer},
        {:active, false},
        {:reuseaddr, true},
        {:packet, 0},
        {:cacertfile, ~c"test/fixtures/server.pem"},
        {:certfile, ~c"test/fixtures/server.crt"},
        {:keyfile, ~c"test/fixtures/server.key"}
      ])

    spawn_link(fn -> listen(listen_socket) end)
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
