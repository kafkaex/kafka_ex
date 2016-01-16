ExUnit.start()

ExUnit.configure exclude: [integration: true, consumer_group: true]

defmodule TestHelper do
  def generate_random_string(string_length \\ 20) do
    :random.seed(:os.timestamp)
    Enum.map(1..string_length, fn _ -> (:random.uniform * 25 + 65) |> round end) |> to_string
  end

  def uris do
    Application.get_env(:kafka_ex, :brokers)
  end

  def utc_time do
    {x, {a,b,c}} = :calendar.local_time |> :calendar.local_time_to_universal_time_dst |> hd
    {x, {a,b,c + 60}}
  end

  def latest_offset_number(topic, partition_id, worker \\ KafkaEx.Server) do
    KafkaEx.latest_offset(topic, partition_id, worker)
    |> first_partition_offset
  end

  defp first_partition_offset(:topic_not_found) do
    nil
  end
  defp first_partition_offset(response) do
    [%KafkaEx.Protocol.Offset.Response{partition_offsets: partition_offsets}] =
      response
    first_partition = hd(partition_offsets)
    first_partition.offset |> hd
  end
end


defmodule ZookeeperDaemon do
  use GenServer
  require Logger


  defstruct bin_dir: nil, dir: nil, pid: nil


  def start_link(connect_params \\ [], opts \\ []) do
    GenServer.start_link(__MODULE__, connect_params, opts)
  end

  def stop(server) do
    GenServer.call(server, :stop)
  end


  def init(params) do
    Process.flag(:trap_exit, true)

    port = Keyword.get(params, :port, 7000)

    dir = Path.join(System.tmp_dir, "zookeeper-#{port}")

    file = create_conf_file(dir, port)
    bin_dir = Keyword.get(params, :cd, "")
    pid = start_daemon(bin_dir, file)

    Logger.debug("started zookeeper @ #{port} in #{dir}, pid: #{pid}")
    {:ok, %__MODULE__{bin_dir: bin_dir, dir: dir, pid: pid}}
  end

  def terminate(_reason, state) do
    Logger.debug("terminate zookeeper, pid: #{state.pid}")
    System.cmd "kill", ["-9", "#{state.pid}"]
    File.rm_rf!(state.dir)
  end

  def handle_call(:stop, _from, state) do
    {:stop, :normal, :ok, state}
  end


  defp start_daemon(bin_dir, server_file) do
    spawn_link(fn -> System.cmd Path.join(bin_dir, "zookeeper-server-start.sh"), [server_file] end)
    :timer.sleep(300)

    {lines, 0} = System.cmd "ps", ["ux"]
    pid = lines
          |> String.split("\n")
          |> Enum.filter(fn x -> String.contains?(x, server_file) && String.contains?(x, "java") && not String.contains?(x, "grep") end)
          |> hd
          |> String.split
          |> Enum.fetch!(1)
          |> String.to_integer

    pid
  end

  defp create_conf_file(dir, port) do
    file = Path.join(dir, "zoo.properties")

    File.mkdir_p!(dir)
    File.write!(file, Enum.join([
      "clientPort=#{port}",
      "dataDir=#{dir}",
      "tickTime=2000",
    ], "\n"))

    file
  end
end


defmodule KafkaDaemon do
  use GenServer
  require Logger


  def start_link(connect_params \\ [], opts \\ []) do
    GenServer.start_link(__MODULE__, connect_params, opts)
  end

  def stop(server) do
    GenServer.call(server, :stop)
  end


  defstruct pid: nil, dir: nil, port: nil, zookeeper_port: nil, bin_dir: nil

  def init(params) do
    Process.flag(:trap_exit, true)

    port = Keyword.get(params, :port, 7001)
    zookeeper_port = Keyword.get(params, :zookeeper_port, 7000)
    broker_id = Keyword.get(params, :broker_id, 1000)

    dir = Path.join(System.tmp_dir, "kafka-#{port}")
    bin_dir = Keyword.get(params, :cd, "")

    {logs_dir, server_file, _, _} = create_conf_file(dir, broker_id, port, zookeeper_port)
    pid = start_daemon(bin_dir, logs_dir, server_file)
    Logger.debug("started kafka @ #{port} in #{dir}, pid: #{pid}")

    {:ok, %__MODULE__{pid: pid, dir: dir, port: port, zookeeper_port: zookeeper_port, bin_dir: bin_dir}}
  end

  def terminate(_reason, state) do
    Logger.debug("terminate kafka, pid: #{state.pid}")
    System.cmd "kill", ["-9", "#{state.pid}"]
    File.rm_rf!(state.dir)
  end

  def handle_call(:stop, _from, state) do
    {:stop, :normal, :ok, state}
  end

  defp start_daemon(bin_dir, logs_dir, server_file) do
    spawn_link(fn -> System.cmd Path.join(bin_dir, "kafka-server-start.sh"), [server_file], env: [{"LOG_DIR", logs_dir}] end)
    :timer.sleep(5000)

    {lines, 0} = System.cmd "ps", ["ux"]
    pid = lines
          |> String.split("\n")
          |> Enum.filter(fn x -> String.contains?(x, logs_dir) && String.contains?(x, "java") && not String.contains?(x, "grep") end)
          |> hd
          |> String.split
          |> Enum.fetch!(1)
          |> String.to_integer

    pid
  end

  defp create_conf_file(dir, broker_id, port, zookeeper_port) do
    logs_dir = Path.join(dir, "logs")
    File.mkdir_p!(logs_dir)

    server_file = Path.join(dir, "server.properties")
    File.write!(server_file, Enum.join([
      "broker.id=#{broker_id}",
      "port=#{port}",
      "host.name=localhost",
      "num.network.threads=3",
      "num.io.threads=8",
      "socket.send.buffer.bytes=102400",
      "socket.receive.buffer.bytes=102400",
      "socket.request.max.bytes=104857600",
      "num.partitions=1",
      "num.recovery.threads.per.data.dir=1",
      "log.retention.hours=168",
      "log.segment.bytes=1073741824",
      "log.retention.check.interval.ms=300000",
      "log.cleaner.enable=false",
      "log.dirs=#{logs_dir}",
      "zookeeper.connect=localhost:#{zookeeper_port}",
      "zookeeper.connection.timeout.ms=6000",
      "auto.create.topics.enable=true"
    ], "\n"))

    consumer_file = Path.join(dir, "consumer.properties")
    File.write!(consumer_file, Enum.join([
      "zookeeper.connect=localhost:#{zookeeper_port}",
      "group.id=test-consumer-group",
    ], "\n"))

    producer_file = Path.join(dir, "producer.properties")
    File.write!(producer_file, Enum.join([
      "metadata.broker.list=localhost:#{port}",
      "producer.type=sync",
      "compression.codec=none",
      "serializer.class=kafka.serializer.DefaultEncoder",
    ], "\n"))

    {logs_dir, server_file, consumer_file, producer_file}
  end

end

