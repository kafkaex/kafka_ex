defmodule KafkaEx.TestSupport.Server do
  def start(port) do
    {:ok, listen_socket} =
      :gen_tcp.listen(port, [
        :binary,
        {:active, false},
        {:reuseaddr, true},
        {:packet, 0}
      ])

    spawn_link(fn -> listen(listen_socket) end)
  end

  defp listen(socket) do
    {:ok, conn} = :gen_tcp.accept(socket)
    spawn_link(fn -> recv(conn) end)
    listen(socket)
  end

  defp recv(conn) do
    case :gen_tcp.recv(conn, 0) do
      {:ok, data} ->
        :ok = :gen_tcp.send(conn, data)

      {:error, :closed} ->
        :ok
    end
  end
end
