defmodule KafkaEx.TestSupport.SSLServer do
  def start(port) do
    {:ok, listen_socket} =
      :ssl.listen(port, [
        :binary,
        {:verify, :verify_none},
        {:active, false},
        {:reuseaddr, true},
        {:packet, 0},
        {:certfile, 'test/fixtures/server.crt'},
        {:keyfile, 'test/fixtures/server.key'}
      ])

    spawn_link(fn -> listen(listen_socket) end)
  end

  defp listen(socket) do
    case :ssl.transport_accept(socket) do
      {:ok, conn} ->
        if otp_version_21_plus?() do
          {:ok, _socket} = :ssl.handshake(conn)
        else
          :ok = :ssl.ssl_accept(conn)
        end

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

  defp otp_version_21_plus? do
    {version, _} = System.otp_release() |> Float.parse()
    version >= 21
  end
end
