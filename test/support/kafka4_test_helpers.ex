defmodule KafkaEx.Kafka4TestHelpers do
  @moduledoc """
  Testcontainers harness for a single-node KRaft Apache Kafka 4.x broker.

  The docker-compose cluster used by the integration suites runs cp-kafka 7.x
  (Kafka 3.x), which negotiates the pre-KIP-848 API versions. Tests tagged
  `:kafka4` run against this container instead, so the newer protocol
  versions (Metadata v13, OffsetCommit/OffsetFetch v10, FindCoordinator v6,
  ConsumerGroupHeartbeat, ...) are exercised end to end.

  Run with `mix test.kafka4` (Docker required; `:kafka4` is excluded by
  default). The image defaults to `apache/kafka:4.4.0` and can be overridden
  with `KAFKA4_IMAGE`.

  ## How it works

  Kafka must advertise the host-mapped port, which is only known after the
  container starts. As in `KafkaEx.ChaosTestHelpers`, the container is started
  with a command that waits for a startup script; once the mapped port is
  known the script is uploaded, exporting `KAFKA_ADVERTISED_LISTENERS` and
  exec-ing the image's own `/etc/kafka/docker/run`.
  """

  require Logger

  alias Testcontainers.Container
  alias Testcontainers.Docker

  @default_image "apache/kafka:4.4.0"
  @external_port 9092
  @internal_port 19092
  @controller_port 9093
  @start_script_dir "/tmp"
  @start_script_name "kafka4-start.sh"

  @doc """
  Starts the Kafka 4 container and waits until it serves requests.

  Returns `{:ok, ctx}` with `:container`, `:host`, `:port` and `:uris`
  (`[{host, port}]`, suitable for `KafkaEx.API.start_client(brokers: ...)`).
  """
  @spec start_kafka(Keyword.t()) :: {:ok, map()}
  def start_kafka(opts \\ []) do
    case Testcontainers.start() do
      {:ok, _} -> :ok
      {:error, {:already_started, _}} -> :ok
    end

    image = Keyword.get(opts, :image, System.get_env("KAFKA4_IMAGE", @default_image))
    {:ok, container} = Testcontainers.start_container(build_container(image))

    host = Testcontainers.get_host()
    port = Container.mapped_port(container, @external_port)

    :ok = upload_start_script(container, host, port)
    Logger.info("Started #{image} (#{container.container_id}); advertised at #{host}:#{port}")

    :ok = wait_for_broker(host, port)
    :ok = wait_for_group_coordinator(host, port)

    {:ok, %{container: container, host: host, port: port, uris: [{host, port}], image: image}}
  end

  @doc "Stops the container started by `start_kafka/1`."
  @spec stop_kafka(map()) :: :ok
  def stop_kafka(%{container: container}) do
    Testcontainers.stop_container(container.container_id)
    :ok
  end

  def stop_kafka(_), do: :ok

  @doc """
  Starts an unnamed `KafkaEx.Client` connected to the Kafka 4 broker.
  """
  @spec start_client(map(), Keyword.t()) :: GenServer.on_start()
  def start_client(%{uris: uris}, opts \\ []) do
    [uris: uris, consumer_group: :no_consumer_group, use_ssl: false, ssl_options: []]
    |> Keyword.merge(opts)
    |> KafkaEx.Client.start_link(:no_name)
  end

  @doc """
  Default `KafkaEx.Consumer.ConsumerGroup` options pointing at the Kafka 4 broker.
  """
  @spec consumer_group_opts(map(), Keyword.t()) :: Keyword.t()
  def consumer_group_opts(%{uris: uris}, opts \\ []) do
    Keyword.merge(
      [
        uris: uris,
        use_ssl: false,
        ssl_options: [],
        heartbeat_interval: 1_000,
        session_timeout: 30_000,
        commit_interval: 500,
        auto_offset_reset: :earliest
      ],
      opts
    )
  end

  # ---------------------------------------------------------------------------

  defp build_container(image) do
    Container.new(image)
    |> Container.with_exposed_port(@external_port)
    |> Container.with_hostname("kafka4")
    |> Container.with_environment("KAFKA_NODE_ID", "1")
    |> Container.with_environment("KAFKA_PROCESS_ROLES", "broker,controller")
    |> Container.with_environment(
      "KAFKA_LISTENERS",
      "PLAINTEXT://0.0.0.0:#{@external_port},BROKER://0.0.0.0:#{@internal_port},CONTROLLER://0.0.0.0:#{@controller_port}"
    )
    |> Container.with_environment(
      "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
      "PLAINTEXT:PLAINTEXT,BROKER:PLAINTEXT,CONTROLLER:PLAINTEXT"
    )
    |> Container.with_environment("KAFKA_INTER_BROKER_LISTENER_NAME", "BROKER")
    |> Container.with_environment("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
    |> Container.with_environment("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:#{@controller_port}")
    |> Container.with_environment("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
    |> Container.with_environment("KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS", "1")
    |> Container.with_environment("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
    |> Container.with_environment("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
    |> Container.with_environment("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
    |> Container.with_environment("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "true")
    |> Container.with_environment("KAFKA_NUM_PARTITIONS", "1")
    |> Container.with_cmd([
      "sh",
      "-c",
      "while [ ! -f #{script_path()} ]; do sleep 0.1; done; exec bash #{script_path()}"
    ])
  end

  defp upload_start_script(container, host, port) do
    script = """
    export KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://#{host}:#{port},BROKER://kafka4:#{@internal_port}
    exec /etc/kafka/docker/run
    """

    {conn, _url, _host} = Testcontainers.Connection.get_connection([])
    Docker.Api.put_file(container.container_id, conn, @start_script_dir, @start_script_name, script)
  end

  defp script_path, do: Path.join(@start_script_dir, @start_script_name)

  # Ready = the broker answers ApiVersions and a Metadata request lists itself.
  # Probed over a raw socket: KafkaEx.Client.init raises while the broker is
  # still booting, which would take the (linked) caller down with it.
  defp wait_for_broker(host, port, attempts \\ 120)

  defp wait_for_broker(host, port, 0) do
    raise "Kafka 4 broker at #{host}:#{port} did not become ready"
  end

  defp wait_for_broker(host, port, attempts) do
    if broker_ready?(host, port) do
      :ok
    else
      Process.sleep(1_000)
      wait_for_broker(host, port, attempts - 1)
    end
  end

  defp broker_ready?(host, port) do
    with {:ok, socket} <- :gen_tcp.connect(~c"#{host}", port, [:binary, active: false, packet: 4], 2_000) do
      try do
        api_versions_ok?(socket) and metadata_has_brokers?(socket)
      after
        :gen_tcp.close(socket)
      end
    else
      _ -> false
    end
  end

  # A fresh broker creates __consumer_offsets on the first FindCoordinator and
  # answers COORDINATOR_NOT_AVAILABLE until it is loaded; wait for that here so
  # tests don't race it.
  defp wait_for_group_coordinator(host, port, attempts \\ 60)

  defp wait_for_group_coordinator(host, port, 0) do
    raise "Kafka 4 broker at #{host}:#{port} never elected a group coordinator"
  end

  defp wait_for_group_coordinator(host, port, attempts) do
    request = %Kayrock.FindCoordinator.V1.Request{key: "kafka4-probe", key_type: 0}

    with {:ok, socket} <- :gen_tcp.connect(~c"#{host}", port, [:binary, active: false, packet: 4], 2_000),
         result = round_trip(socket, request),
         :ok <- :gen_tcp.close(socket),
         {:ok, %{error_code: 0}} <- result do
      :ok
    else
      _ ->
        Process.sleep(500)
        wait_for_group_coordinator(host, port, attempts - 1)
    end
  end

  defp api_versions_ok?(socket) do
    match?({:ok, %{error_code: 0}}, round_trip(socket, %Kayrock.ApiVersions.V0.Request{}))
  end

  defp metadata_has_brokers?(socket) do
    case round_trip(socket, %Kayrock.Metadata.V1.Request{topics: []}) do
      {:ok, %{brokers: [_ | _]}} -> true
      _ -> false
    end
  end

  defp round_trip(socket, request) do
    request = %{request | correlation_id: 1, client_id: "kafka4-probe"}

    with :ok <- :gen_tcp.send(socket, Kayrock.Request.serialize(request)),
         {:ok, data} <- :gen_tcp.recv(socket, 0, 5_000) do
      {response, _rest} = Kayrock.Request.response_deserializer(request).(data)
      {:ok, response}
    end
  rescue
    _ -> :error
  end
end
