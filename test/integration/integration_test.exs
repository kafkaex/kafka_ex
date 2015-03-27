defmodule KafkaEx.Integration.Test do
  use ExUnit.Case
  @moduletag :integration

  test "KafkaEx.Server starts on Application start up" do
    pid = Process.whereis(KafkaEx.Server)
    assert is_pid(pid)
  end

  #create_worker
  test "KafkaEx.Supervisor dynamically creates workers" do
    {:ok, pid} = KafkaEx.create_worker(:bar, uris)
    assert Process.whereis(:bar) == pid
  end

  test "KafkaEx.Server connects to all supplied brokers" do
    pid = Process.whereis(KafkaEx.Server)
    {_, _metadata, socket_map, _} = :sys.get_state(pid)
    assert Enum.sort(Map.keys(socket_map)) == Enum.sort(uris)
  end

  test "KafkaEx.Server generates metadata on start up" do
    pid = Process.whereis(KafkaEx.Server)
    {_, metadata, _socket_map, _} = :sys.get_state(pid)
    refute metadata == %{}

    brokers = Map.values(metadata[:brokers])

    assert Enum.sort(brokers) == Enum.sort(uris)
  end

  test "start_link creates the server and registers it as the module name" do
    {:ok, pid} = KafkaEx.create_worker(:test_server, uris)
    assert pid == Process.whereis(:test_server)
  end

  test "start_link raises an exception when it is provided a bad connection" do
    {:error, {exception, _}} = KafkaEx.create_worker(:no_host, [{"bad_host", 1000}])
    assert exception.__struct__ == KafkaEx.ConnectionError
    assert exception.message == "Error: Cannot connect to any of the broker(s) provided"
  end

  #produce
  test "produce withiout an acq required returns :ok" do
    assert KafkaEx.produce("food", 0, "hey") == :ok
  end

  test "produce with ack required returns an ack" do
    {:ok, %{"food" => %{0 => %{error_code: 0, offset: offset}}}} =  KafkaEx.produce("food", 0, "hey", KafkaEx.Server, nil, 1)
    refute offset == nil
  end

  test "produce updates metadata" do
    pid = Process.whereis(KafkaEx.Server)
    :sys.replace_state(pid, fn({correlation_id, _metadata, socket_map, _}) -> {correlation_id, %{}, socket_map, nil} end)
    KafkaEx.produce("food", 0, "hey")
    {_, metadata, _socket_map, _} = :sys.get_state(pid)
    refute metadata == %{}

    brokers = Map.values(metadata[:brokers])

    assert Enum.sort(brokers) == Enum.sort(uris)
  end

  test "produce creates log for a non-existing topic" do
    random_string = generate_random_string
    KafkaEx.produce(random_string, 0, "hey")
    pid = Process.whereis(KafkaEx.Server)
    {_, metadata, _socket_map, _} = :sys.get_state(pid)
    random_topic_metadata_found = metadata[:topics] |> Map.keys |> Enum.member?(random_string)

    assert random_topic_metadata_found
  end

  #metadata
  test "metadata attempts to connect via one of the exisiting sockets" do
    {:ok, pid} = KafkaEx.create_worker(:one_working_port, uris)
    {_, _metadata, socket_map, _} = :sys.get_state(pid)
    [_ |rest] = Map.values(socket_map) |> Enum.reverse
    Enum.each(rest, &:gen_tcp.close/1)
    brokers = KafkaEx.metadata("", :one_working_port)[:brokers] |> Map.values
    assert Enum.sort(brokers) == Enum.sort(uris)
  end

  test "metadata for a non-existing topic creates a new topic" do
    random_string = generate_random_string
    random_topic_metadata = KafkaEx.metadata(random_string)[:topics][random_string]
    assert random_topic_metadata[:error_code] == 0
    refute random_topic_metadata[:partitions] == %{}

    pid = Process.whereis(KafkaEx.Server)
    {_, metadata, _socket_map, _} = :sys.get_state(pid)
    random_topic_metadata_found = metadata[:topics] |> Map.keys |> Enum.member?(random_string)

    assert random_topic_metadata_found
  end

  #fetch
  test "fetch updates metadata" do
    pid = Process.whereis(KafkaEx.Server)
    :sys.replace_state(pid, fn({correlation_id, _metadata, socket_map, _}) -> {correlation_id, %{}, socket_map, nil} end)
    KafkaEx.fetch("food", 0, 0)
    {_, metadata, _socket_map, _} = :sys.get_state(pid)
    refute metadata == %{}

    brokers = Map.values(metadata[:brokers])

    assert Enum.sort(brokers) == Enum.sort(uris)
  end

  test "fetch retrieves empty logs for non-exisiting topic" do
    random_string = generate_random_string
    {:ok, map} = KafkaEx.fetch(random_string, 0, 0)
    %{0 => %{message_set: message_set}} = Map.get(map, random_string)

    assert message_set == []
  end

  test "fetch works" do
    {:ok, %{"food" => %{0 => %{error_code: 0, offset: offset}}}} =  KafkaEx.produce("food", 0, "hey foo", KafkaEx.Server, nil, 1)
    {:ok, %{"food" => %{0 => %{message_set: message_set}}}} = KafkaEx.fetch("food", 0, 0)
    message = message_set |> Enum.reverse |> hd

    assert message.value == "hey foo"
    assert message.offset == offset
  end

  #offset
  test "offset updates metadata" do
    pid = Process.whereis(KafkaEx.Server)
    :sys.replace_state(pid, fn({correlation_id, _metadata, socket_map, _}) -> {correlation_id, %{}, socket_map, nil} end)
    KafkaEx.latest_offset("food", 0)
    {_, metadata, _socket_map, _} = :sys.get_state(pid)
    refute metadata == %{}

    brokers = Map.values(metadata[:brokers])

    assert Enum.sort(brokers) == Enum.sort(uris)
  end

  test "latest_offset retrieves offset of 0 for non-existing topic" do
    random_string = generate_random_string
    {:ok, map} = KafkaEx.latest_offset(random_string, 0)
    %{0 => %{offsets: [offset]}} = Map.get(map, random_string)

    assert offset == 0
  end

  #stream
  test "streams kafka logs" do
    random_string = generate_random_string
    KafkaEx.create_worker(:stream, uris)
    KafkaEx.produce(random_string, 0, "hey", :stream)
    KafkaEx.produce(random_string, 0, "hi", :stream)
    log = KafkaEx.stream(random_string, 0, :stream) |> Enum.take(2)

    refute Enum.empty?(log)
    [first,second|_] = log
    assert first.value == "hey"
    assert second.value == "hi"
  end

  def uris do
    Mix.Config.read!("config/config.exs") |> hd |> elem(1) |> hd |> elem(1)
  end

  def generate_random_string(string_length \\ 20) do
    :random.seed(:os.timestamp)
    Enum.map(1..string_length, fn _ -> (:random.uniform * 25 + 65) |> round end) |> to_string
  end
end
