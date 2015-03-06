defmodule Kafka.Integration.Test do
  use ExUnit.Case
  @moduletag :integration

  test "Kafka.Server starts on Application start up" do
    pid = Process.whereis(Kafka.Server)
    assert is_pid(pid)
  end

  #create_worker
  test "Kafka.Supervisor dynamically creates workers" do
    pid = Kafka.Supervisor.create_worker(uris, :bar)
    assert Process.whereis(:bar) == pid
  end

  test "Kafka.Server connects to all supplied brokers" do
    pid = Process.whereis(Kafka.Server)
    {_, _metadata, socket_map, _} = :sys.get_state(pid)
    assert Enum.sort(Map.keys(socket_map)) == Enum.sort(uris)
  end

  test "Kafka.Server generates metadata on start up" do
    pid = Process.whereis(Kafka.Server)
    {_, metadata, _socket_map, _} = :sys.get_state(pid)
    refute metadata == %{}

    brokers = Map.values(metadata[:brokers])

    assert Enum.sort(brokers) == Enum.sort(uris)
  end

  test "start_link creates the server and registers it as the module name" do
    {:ok, pid} = Kafka.Server.start_link(uris, :test_server)
    assert pid == Process.whereis(:test_server)
  end

  test "start_link raises an exception when it is provided a bad connection" do
    assert_link_exit(Kafka.ConnectionError, "Error: Cannot connect to any of the broker(s) provided", fn -> Kafka.Server.start_link([{"bad_host", 1000}], :no_host) end)
  end

  #produce
  test "produce withiout an acq required returns :ok" do
    assert Kafka.Server.produce("food", 0, "hey") == :ok
  end

  test "produce with ack required returns an ack" do
    {:ok, %{"food" => %{0 => %{error_code: 0, offset: offset}}}} =  Kafka.Server.produce("food", 0, "hey", Kafka.Server, nil, 1)
    refute offset == nil
  end

  test "produce updates metadata" do
    pid = Process.whereis(Kafka.Server)
    :sys.replace_state(pid, fn({correlation_id, _metadata, socket_map, _}) -> {correlation_id, %{}, socket_map, nil} end)
    Kafka.Server.produce("food", 0, "hey")
    {_, metadata, _socket_map, _} = :sys.get_state(pid)
    refute metadata == %{}

    brokers = Map.values(metadata[:brokers])

    assert Enum.sort(brokers) == Enum.sort(uris)
  end

  test "produce creates log for a non-existing topic" do
    random_string = generate_random_string
    Kafka.Server.produce(random_string, 0, "hey")
    pid = Process.whereis(Kafka.Server)
    {_, metadata, _socket_map, _} = :sys.get_state(pid)
    random_topic_metadata_found = metadata[:topics] |> Map.keys |> Enum.member?(random_string)

    assert random_topic_metadata_found
  end

  #metadata
  test "metadata attempts to connect via one of the exisiting sockets" do
    {:ok, pid} = Kafka.Server.start_link(uris, :one_working_port)
    {_, _metadata, socket_map, _} = :sys.get_state(pid)
    [_ |rest] = Map.values(socket_map) |> Enum.reverse
    Enum.each(rest, &:gen_tcp.close/1)
    brokers = Kafka.Server.metadata("", :one_working_port)[:brokers] |> Map.values
    assert Enum.sort(brokers) == Enum.sort(uris)
  end

  test "metadata for a non-existing topic creates a new topic" do
    random_string = generate_random_string
    random_topic_metadata = Kafka.Server.metadata(random_string)[:topics][random_string]
    assert random_topic_metadata[:error_code] == 0
    refute random_topic_metadata[:partitions] == %{}

    pid = Process.whereis(Kafka.Server)
    {_, metadata, _socket_map, _} = :sys.get_state(pid)
    random_topic_metadata_found = metadata[:topics] |> Map.keys |> Enum.member?(random_string)

    assert random_topic_metadata_found
  end

  #fetch
  test "fetch updates metadata" do
    pid = Process.whereis(Kafka.Server)
    :sys.replace_state(pid, fn({correlation_id, _metadata, socket_map, _}) -> {correlation_id, %{}, socket_map, nil} end)
    Kafka.Server.fetch("food", 0, 0)
    {_, metadata, _socket_map, _} = :sys.get_state(pid)
    refute metadata == %{}

    brokers = Map.values(metadata[:brokers])

    assert Enum.sort(brokers) == Enum.sort(uris)
  end

  test "fetch retrieves empty logs for non-exisiting topic" do
    random_string = generate_random_string
    {:ok, map} = Kafka.Server.fetch(random_string, 0, 0)
    %{0 => %{message_set: message_set}} = Map.get(map, random_string)

    assert message_set == []
  end

  test "fetch works" do
    {:ok, %{"food" => %{0 => %{error_code: 0, offset: offset}}}} =  Kafka.Server.produce("food", 0, "hey foo", Kafka.Server, nil, 1)
    {:ok, %{"food" => %{0 => %{message_set: message_set}}}} = Kafka.Server.fetch("food", 0, 0)
    message = message_set |> Enum.reverse |> hd

    assert message.value == "hey foo"
    assert message.offset == offset
  end

  #offset
  test "offset updates metadata" do
    pid = Process.whereis(Kafka.Server)
    :sys.replace_state(pid, fn({correlation_id, _metadata, socket_map, _}) -> {correlation_id, %{}, socket_map, nil} end)
    Kafka.Server.latest_offset("food", 0)
    {_, metadata, _socket_map, _} = :sys.get_state(pid)
    refute metadata == %{}

    brokers = Map.values(metadata[:brokers])

    assert Enum.sort(brokers) == Enum.sort(uris)
  end

  test "latest_offset retrieves offset of 0 for non-existing topic" do
    random_string = generate_random_string
    {:ok, map} = Kafka.Server.latest_offset(random_string, 0)
    %{0 => %{offsets: [offset]}} = Map.get(map, random_string)

    assert offset == 0
  end

  #stream
  test "streams kafka logs" do
    random_string = generate_random_string
    Kafka.Server.start_link(uris, :stream)
    Kafka.Server.produce(random_string, 0, "hey", :stream)
    Kafka.Server.produce(random_string, 0, "hi", :stream)
    log = Kafka.Server.stream(random_string, 0, :stream) |> Enum.take(2)

    refute Enum.empty?(log)
    [first,second|_] = log
    assert first.value == "hey"
    assert second.value == "hi"
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

  def generate_random_string(string_length \\ 20) do
    :random.seed(:os.timestamp)
    Enum.map(1..string_length, fn _ -> (:random.uniform * 25 + 65) |> round end) |> to_string
  end

  def assert_link_exit(exception, function) when is_function(function) do
    Process.flag(:trap_exit, true)
    function.()
    error = receive do
      {:EXIT, _, {:function_clause, [{error, :exception, [message], _}|_]}} -> Map.put(error.__struct__, :message, message)
      {:EXIT, _, {:undef, [{module, function, args, _}]}} -> %UndefinedFunctionError{module: module, function: function, arity: length(args)}
      {:EXIT, _, {:function_clause, [{module, function, args, _}|_]}} -> %FunctionClauseError{module: module, function: function, arity: length(args)}
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
