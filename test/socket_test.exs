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

  defmodule SSLServer do
    def start(port) do
      {:ok, listen_socket} = :ssl.listen(port, [:binary, {:active, false}, {:reuseaddr, true}, {:packet, 0}, {:certfile, 'test/fixtures/server.crt'}, {:keyfile, 'test/fixtures/server.key'}])
      spawn_link(fn -> listen(listen_socket) end)
    end

    defp listen(socket) do
       case :ssl.transport_accept(socket) do
        {:ok, conn} ->
          :ok = :ssl.ssl_accept(conn)
          pid = spawn_link(fn -> recv(conn) end)
          :ssl.controlling_process(socket, pid)
        _ -> :ok
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

  setup_all do
    :ssl.start
    SSLServer.start(3030)
    Server.start(3040)
    {:ok, [ssl_port: 3030, port: 3040]}
  end

  test "create a non SSL socket", context do
    {:ok, socket} = KafkaEx.Socket.create('localhost', context[:port], [:binary, {:packet, 0}], false)
    assert socket.ssl == false
    KafkaEx.Socket.close(socket)
  end

  test "send and receive using a non SSL socket", context do
    {:ok, socket} = KafkaEx.Socket.create('localhost', context[:port], [:binary, {:packet, 0}, {:active, false}], false)
    KafkaEx.Socket.send(socket, 'ping')
    assert {:ok, "ping" } == KafkaEx.Socket.recv(socket, 0)
    KafkaEx.Socket.close(socket)
  end

  test "retrieve info from a non SSL socket", context do
    {:ok, socket} = KafkaEx.Socket.create('localhost', context[:port], [:binary, {:packet, 0}, {:active, false}], false)
    info = KafkaEx.Socket.info(socket)
    assert info[:name] == 'tcp_inet'
    KafkaEx.Socket.close(socket)
    assert {:error, :closed} == KafkaEx.Socket.send(socket, 'ping')
  end

  test "retrieve nil info from nil socket", context do
    info = KafkaEx.Socket.info(nil)
    assert is_nil(info) == true
  end

  test "create a SSL socket", context do
    {:ok, socket} = KafkaEx.Socket.create('localhost', context[:ssl_port], [:binary, {:packet, 0}], true)
    assert socket.ssl == true
    KafkaEx.Socket.close(socket)
  end

  test "send and receive using a SSL socket", context do
    {:ok, socket} = KafkaEx.Socket.create('localhost', context[:ssl_port], [:binary, {:packet, 0}, {:active, false}], true)
    KafkaEx.Socket.send(socket, 'ping')
    assert {:ok, "ping" } == KafkaEx.Socket.recv(socket, 0)
    KafkaEx.Socket.close(socket)
  end

  test "retrieve info from a SSL socket", context do
    {:ok, socket} = KafkaEx.Socket.create('localhost', context[:ssl_port], [:binary, {:packet, 0}, {:active, false}], true)
    info = KafkaEx.Socket.info(socket)
    assert info[:name] == 'tcp_inet'
    KafkaEx.Socket.close(socket)
    assert {:error, :closed} == KafkaEx.Socket.send(socket, 'ping')
  end
end
