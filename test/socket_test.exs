defmodule KafkaEx.Socket.Test do
  use ExUnit.Case, async: false

  defmodule Server do
    def start(port) do
      {:ok, listen_socket} = :gen_tcp.listen(port, [:binary, {:active, false}, {:reuseaddr, true}, {:packet, 0}])
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

  setup_all do
    Server.start(3040)
    {:ok, [port: 3040]}
  end

  test "create, send and receive", context do
    {:ok, socket} = KafkaEx.Socket.create('localhost', context[:port], [:binary, {:packet, 0}, {:active, false}])
    KafkaEx.Socket.send(socket, 'ping')
    assert {:ok, "ping" } == KafkaEx.Socket.recv(socket, 0)
    KafkaEx.Socket.close(socket)
  end

  test "info", context do
    {:ok, socket} = KafkaEx.Socket.create('localhost', context[:port], [:binary, {:packet, 0}, {:active, false}])
    info = KafkaEx.Socket.info(socket)
    assert info[:name] == 'tcp_inet'
    KafkaEx.Socket.close(socket)
    assert {:error, :closed} == KafkaEx.Socket.send(socket, 'ping')
  end
end
