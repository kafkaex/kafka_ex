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
    assert_link_exit(Kafka.ConnectionError, "Error: Bad broker format ''", fn -> Kafka.Server.start_link(nil, :no_host) end)
  end

  test "start_link raises an exception when it is provided a bad connection" do
    Process.flag(:trap_exit, true)
    assert_link_exit(Kafka.ConnectionError, "Error: Bad broker format ''", fn -> Kafka.Server.start_link(nil, :no_host) end)
  end

  test "start_link handles a non binary host" do
    Process.flag(:trap_exit, true)
    {:ok, _} = Kafka.Server.start_link([{to_char_list(System.get_env("HOST")), String.to_integer(System.get_env("PORT"))}], :char_list_host)
  end

  test "start_link handles a string port" do
    Process.flag(:trap_exit, true)
    {:ok, pid} = Kafka.Server.start_link([{System.get_env("HOST"), System.get_env("PORT")}], :binary_port)
  end

  def uris do
    Mix.Config.read!("config/config.exs") |> hd |> elem(1) |> hd |> elem(1)
  end

  def assert_link_exit(exception, message, function) when is_function(function) do
    error = assert_link_exit(exception, function)
    is_match = cond do
      is_binary(message) -> Exception.message(error) == message
      Regex.regex?(message) -> Exception.message(error) =~ message
    end
    msg = "Wrong message for #{inspect exception}. " <>
    "Expected #{inspect message}, got #{inspect Exception.message(error)}"
    assert is_match, message: msg
    error
  end

  def assert_link_exit(exception, function) when is_function(function) do
    Process.flag(:trap_exit, true)
    function.()
    error = receive do
      {:EXIT, _, {:function_clause, [{error, :exception, [message], _}|_]}} -> Map.put(error.__struct__, :message, message)
      {:EXIT, _, {:undef, [{module, function, args, _}]}} -> %UndefinedFunctionError{module: module, function: function, arity: length(args)}
      {:EXIT, _, {error, _}} -> error
    after
      1000 -> :nothing
    end

    if error == :nothing do
      flunk "Expected exception #{inspect exception} but nothing was raised"
    else
      name = error.__struct__
      cond do
        name == exception ->
          error
        true ->
          flunk "Expected exception #{inspect exception} but got #{inspect name} (#{error.__struct__.message(error)})"
      end
    end
    error
  end

end
