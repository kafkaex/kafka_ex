defmodule Kafka.Integration.Test do
  use ExUnit.Case
  @moduletag :integration

  test "Kafka.Server starts on Application start up" do
    pid = Process.whereis(Kafka.Server)
    assert is_pid(pid)
  end

  test "start_link creates the server and registers it as the module name" do
    {:ok, pid} = Kafka.Server.start_link(uris, :test_server)
    assert pid == Process.whereis(:test_server)
  end

  test "start_link raises an exception when it cannot connect to any of the supplied brokers" do
    Process.flag(:trap_exit, true)
    {:error,
      {%Kafka.ConnectionError{message: message},
        [_, _, _, _]}} = Kafka.Server.start_link([{"bad_host", 9092}], :bad_host)
    assert "Error: Cannot connect to any brokers provided" == message
  end

  test "start_link raises an exception when it is provided a bad connection" do
    Process.flag(:trap_exit, true)
    {:error,
      {%Kafka.ConnectionError{message: message},
        [_, _, _, _]}} = Kafka.Server.start_link(nil, :no_host)
    assert "Error: Bad broker format ''" == message
  end

  test "start_link handles a non binary host" do
    Process.flag(:trap_exit, true)
    {:ok, _} = Kafka.Server.start_link([{'localhost', 9092}], :char_list_host)
  end

  test "start_link handles a string port" do
    Process.flag(:trap_exit, true)
    {:ok, pid} = Kafka.Server.start_link([{"localhost", "9092"}], :binary_port)
  end

  def uris do
    Mix.Config.read!("config/config.exs") |> hd |> elem(1) |> hd |> elem(1)
  end

end
