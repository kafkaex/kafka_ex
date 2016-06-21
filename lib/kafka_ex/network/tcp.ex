defmodule KafkaEx.Network.TCP do

  def connect(host, port) do
    :gen_tcp.connect(host, port, [:binary, packet: 4, active: false])
  end

  def send(socket, data) do
    :gen_tcp.send(socket, data)
  end

  def recv(socket, timeout) do
    :gen_tcp.recv(socket, 0, timeout)
  end

  def close(socket) do
    :ok = :gen_tcp.close(socket)
  end

end
