defmodule KafkaEx.ChaosTestHelpers do
  @moduledoc """
  Helpers for chaos tests using Testcontainers and Toxiproxy.

  Spins up isolated Kafka and Toxiproxy containers on a shared Docker network
  for each test run. Traffic to Kafka always goes through Toxiproxy, enabling
  network fault injection.

  ## Architecture

      Test Client --> Toxiproxy (localhost:PROXY_PORT) --> Kafka (kafka:9092)

  Kafka is configured to advertise the proxy port, so all client connections
  go through the proxy.
  """

  require Logger

  alias Testcontainers.Container
  alias Testcontainers.Docker
  alias Testcontainers.ToxiproxyContainer

  @kafka_image "confluentinc/cp-kafka:7.4.3"
  @kafka_port 9092
  @kafka_broker_port 29092
  @zookeeper_port 2181
  @start_script_path "tc-start.sh"

  @doc """
  Start Kafka and Toxiproxy containers on a shared network for testing.

  Returns a context map with:
  - `:kafka_container` - the Kafka container
  - `:toxiproxy_container` - the Toxiproxy container
  - `:network_name` - the Docker network name
  - `:proxy_name` - the name of the proxy in Toxiproxy
  - `:broker_via_proxy` - the broker address through the proxy `{host, port}`
  """
  def start_infrastructure(opts \\ []) do
    # Handle Testcontainers already started (when multiple test suites run)
    case Testcontainers.start() do
      {:ok, _} -> :ok
      {:error, {:already_started, _}} -> :ok
    end

    network_name = Keyword.get(opts, :network, "kafka-test-#{:rand.uniform(100_000)}")

    {:ok, _} = Testcontainers.create_network(network_name)
    Logger.info("Created Docker network: #{network_name}")
    host = Testcontainers.get_host()

    toxiproxy_config =
      Container.new(ToxiproxyContainer.default_image())
      |> Container.with_exposed_ports([ToxiproxyContainer.control_port() | Enum.to_list(8666..8696)])
      |> Container.with_waiting_strategy(
        Testcontainers.PortWaitStrategy.new(
          "127.0.0.1",
          ToxiproxyContainer.control_port(),
          60_000
        )
      )
      |> Container.with_network(network_name)
      |> Container.with_hostname("toxiproxy")

    {:ok, toxiproxy_container} = Testcontainers.start_container(toxiproxy_config)
    Logger.info("Started Toxiproxy container")

    :ok = ToxiproxyContainer.configure_toxiproxy_ex(toxiproxy_container)
    Logger.info("Toxiproxy API ready at #{ToxiproxyContainer.api_url(toxiproxy_container)}")

    # Step 2: Start Kafka - using startup script approach for dynamic advertised listeners
    # Container waits for script, we upload script after container starts with correct ports
    kafka_config = build_kafka_container(network_name)
    {:ok, kafka_container} = Testcontainers.start_container(kafka_config)
    kafka_ip = kafka_container.ip_address
    Logger.info("Started Kafka container (IP: #{kafka_ip})")

    # Step 3: Configure proxy to route to Kafka's internal IP address
    proxy_name = "kafka_broker"
    upstream = "#{kafka_ip}:#{@kafka_port}"

    {:ok, proxy_port} = ToxiproxyContainer.create_proxy(toxiproxy_container, proxy_name, upstream)

    Logger.info("Created proxy: #{proxy_name} -> #{upstream}")

    # Step 4: Upload the startup script with correct advertised listeners pointing to PROXY
    # This is the key - advertise the PROXY port so all client traffic goes through Toxiproxy
    :ok = upload_kafka_startup_script(kafka_container, kafka_ip, host, proxy_port)
    Logger.info("Uploaded startup script with advertised listener: #{host}:#{proxy_port}")

    # Step 5: Wait for Kafka to be ready (through the proxy)
    :ok = wait_for_kafka(host, proxy_port)
    Logger.info("Kafka ready via proxy at #{host}:#{proxy_port}")

    {:ok,
     %{
       kafka_container: kafka_container,
       toxiproxy_container: toxiproxy_container,
       network_name: network_name,
       proxy_name: proxy_name,
       broker_via_proxy: {host, proxy_port},
       proxy_port: proxy_port
     }}
  end

  @doc """
  Stop infrastructure containers and remove network.
  """
  def stop_infrastructure(%{kafka_container: kafka, toxiproxy_container: toxiproxy, network_name: network_name}) do
    Testcontainers.stop_container(kafka.container_id)
    Testcontainers.stop_container(toxiproxy.container_id)
    Testcontainers.remove_network(network_name)

    :ok
  end

  def stop_infrastructure(_), do: :ok

  # Build Kafka container using startup script approach
  # This is based on testcontainers-java's KafkaContainer implementation
  # The container starts but waits for a startup script before launching Kafka
  # This allows us to configure advertised listeners AFTER we know the proxy port
  defp build_kafka_container(network_name) do
    Container.new(@kafka_image)
    |> Container.with_exposed_port(@kafka_port)
    |> Container.with_network(network_name)
    |> Container.with_hostname("kafka")
    |> Container.with_environment(
      "KAFKA_LISTENERS",
      "BROKER://0.0.0.0:#{@kafka_broker_port},OUTSIDE://0.0.0.0:#{@kafka_port}"
    )
    |> Container.with_environment("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "BROKER:PLAINTEXT,OUTSIDE:PLAINTEXT")
    |> Container.with_environment("KAFKA_INTER_BROKER_LISTENER_NAME", "BROKER")
    |> Container.with_environment("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
    |> Container.with_environment("KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS", "1")
    |> Container.with_environment("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
    |> Container.with_environment("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
    |> Container.with_environment("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "true")
    |> Container.with_cmd([
      "sh",
      "-c",
      "while [ ! -f /#{@start_script_path} ]; do echo 'waiting for startup script...' && sleep 0.1; done; sh /#{@start_script_path};"
    ])
  end

  defp upload_kafka_startup_script(container, kafka_ip, proxy_host, proxy_port) do
    internal_listener = "BROKER://#{kafka_ip}:#{@kafka_broker_port}"
    external_listener = "OUTSIDE://#{proxy_host}:#{proxy_port}"

    script = """
    export KAFKA_BROKER_ID=1
    export KAFKA_ADVERTISED_LISTENERS=#{internal_listener},#{external_listener}
    echo '' > /etc/confluent/docker/ensure
    export KAFKA_ZOOKEEPER_CONNECT='localhost:#{@zookeeper_port}'
    echo 'clientPort=#{@zookeeper_port}' > zookeeper.properties
    echo 'dataDir=/var/lib/zookeeper/data' >> zookeeper.properties
    echo 'dataLogDir=/var/lib/zookeeper/log' >> zookeeper.properties
    zookeeper-server-start zookeeper.properties &
    /etc/confluent/docker/run
    echo finished
    """

    script_content =
      script
      |> String.split("\n")
      |> Enum.map(&String.trim/1)
      |> Enum.reject(&(&1 == ""))
      |> Enum.join("\n")

    # Upload script to container - need connection from testcontainers
    {:ok, conn} = get_docker_connection()
    Docker.Api.put_file(container.container_id, conn, "/", @start_script_path, script_content)
  end

  # Get Docker connection from testcontainers
  defp get_docker_connection do
    # The connection is stored in the testcontainers GenServer state
    # We can get it by calling the connection module directly
    {conn, _url, _host} = Testcontainers.Connection.get_connection([])
    {:ok, conn}
  end

  defp wait_for_kafka(host, port, attempts \\ 120) do
    Logger.info("Waiting for Kafka to be ready at #{host}:#{port} (attempt #{121 - attempts}/120)...")

    case try_kafka_api_versions(host, port) do
      :ok ->
        Logger.info("Kafka is ready and responding to API requests!")
        # Wait longer for __consumer_offsets topic to be created and coordinator to be elected
        # This is critical for consumer group tests
        Logger.info("Waiting for Kafka internal topics to initialize...")
        Process.sleep(5000)

        case try_kafka_api_versions(host, port) do
          :ok ->
            Logger.info("Kafka confirmed ready after settling period")
            :ok

          {:error, reason} ->
            Logger.warning("Kafka became unavailable after settling: #{inspect(reason)}")
            {:error, reason}
        end

      {:error, reason} when attempts > 0 ->
        Logger.debug("Kafka not ready yet: #{inspect(reason)}")
        Process.sleep(1000)
        wait_for_kafka(host, port, attempts - 1)

      {:error, reason} ->
        {:error, {:kafka_not_ready, reason}}
    end
  end

  # Send a Kafka ApiVersions request (API key 18)
  # This is what testcontainers uses to verify Kafka is ready
  defp try_kafka_api_versions(host, port) do
    case :gen_tcp.connect(~c"#{host}", port, [:binary, active: false, packet: :raw], 5000) do
      {:ok, socket} ->
        client_id = "tc-wait"
        client_id_bytes = <<byte_size(client_id)::16>> <> client_id

        request_body = <<18::16-signed, 0::16-signed, 1::32-signed>> <> client_id_bytes

        request = <<byte_size(request_body)::32-signed>> <> request_body

        result =
          with :ok <- :gen_tcp.send(socket, request),
               {:ok, <<_size::32-signed, _correlation_id::32-signed, error_code::16-signed, _rest::binary>>} <-
                 :gen_tcp.recv(socket, 0, 10_000) do
            if error_code == 0 do
              :ok
            else
              {:error, {:kafka_error, error_code}}
            end
          end

        :gen_tcp.close(socket)
        result

      {:error, reason} ->
        {:error, reason}
    end
  end

  # ---------------------------------------------------------------------------
  # Toxic Helpers
  # ---------------------------------------------------------------------------

  @doc """
  Execute function with proxy completely down (connection refused).
  """
  def with_broker_down(proxy_name, fun) do
    proxy_name |> ToxiproxyEx.get!() |> ToxiproxyEx.down!(fun)
  end

  @doc """
  Execute function with added latency on downstream (responses from Kafka).
  """
  def with_latency(proxy_name, latency_ms, fun) do
    proxy_name
    |> ToxiproxyEx.get!()
    |> ToxiproxyEx.toxic(:latency, latency: latency_ms)
    |> ToxiproxyEx.apply!(fun)
  end

  @doc """
  Execute function with connection timeout (data stops, connection stays open).
  """
  def with_timeout(proxy_name, timeout_ms, fun) do
    proxy_name
    |> ToxiproxyEx.get!()
    |> ToxiproxyEx.toxic(:timeout, timeout: timeout_ms)
    |> ToxiproxyEx.apply!(fun)
  end

  @doc """
  Execute function with TCP RST (connection reset).
  """
  def with_reset_peer(proxy_name, delay_ms, fun) do
    proxy_name
    |> ToxiproxyEx.get!()
    |> ToxiproxyEx.toxic(:reset_peer, timeout: delay_ms)
    |> ToxiproxyEx.apply!(fun)
  end

  @doc """
  Execute function with bandwidth limit.
  """
  def with_bandwidth_limit(proxy_name, rate_kb, fun) do
    proxy_name
    |> ToxiproxyEx.get!()
    |> ToxiproxyEx.toxic(:bandwidth, rate: rate_kb)
    |> ToxiproxyEx.apply!(fun)
  end

  @doc """
  Execute function with slow close (delay before connection close).
  """
  def with_slow_close(proxy_name, delay_ms, fun) do
    proxy_name
    |> ToxiproxyEx.get!()
    |> ToxiproxyEx.toxic(:slow_close, delay: delay_ms)
    |> ToxiproxyEx.apply!(fun)
  end

  @doc """
  Reset all toxics and re-enable proxies.
  """
  def reset_all do
    ToxiproxyEx.reset!()
  rescue
    _ -> :ok
  end

  # ---------------------------------------------------------------------------
  # Client helpers
  # ---------------------------------------------------------------------------

  @doc """
  Start a KafkaEx client using testcontainers context.

  Uses KafkaEx.Client.start_link directly to avoid depending on KafkaEx.Supervisor.
  Each client gets a unique name based on the test process to avoid collisions.
  """
  def start_client(%{broker_via_proxy: broker}, opts \\ []) do
    # Generate unique name based on the calling process
    client_name = client_name_for_test()

    default_opts = [
      uris: [broker],
      consumer_group: :no_consumer_group,
      use_ssl: false,
      ssl_options: []
    ]

    merged_opts = Keyword.merge(default_opts, opts)

    # Use KafkaEx.Client.start_link directly to avoid supervisor dependency
    case KafkaEx.Client.start_link(merged_opts, client_name) do
      {:ok, pid} ->
        {:ok, pid}

      {:error, {:already_started, pid}} ->
        # Client still running, stop and restart
        GenServer.stop(pid, :normal, 5000)
        Process.sleep(100)
        KafkaEx.Client.start_link(merged_opts, client_name)

      error ->
        error
    end
  end

  @doc """
  Stop the test client.
  """
  def stop_client do
    client_name = client_name_for_test()
    stop_client_by_name(client_name)
  end

  @doc """
  Stop a specific client by name.
  """
  def stop_client_by_name(name) do
    case Process.whereis(name) do
      nil ->
        :ok

      pid ->
        try do
          GenServer.stop(pid, :normal, 5000)
          # Wait for process to fully terminate
          wait_for_process_exit(pid, 1000)
        catch
          :exit, _ -> :ok
        end
    end
  end

  # Generate a unique client name for the current test process
  defp client_name_for_test do
    # Use a deterministic name based on test module + test name stored in process dictionary
    # or fall back to pid-based name
    case Process.get(:chaos_client_name) do
      nil ->
        name = :"chaos_client_#{:erlang.phash2(self())}"
        Process.put(:chaos_client_name, name)
        name

      name ->
        name
    end
  end

  defp wait_for_process_exit(pid, timeout) when timeout > 0 do
    if Process.alive?(pid) do
      Process.sleep(50)
      wait_for_process_exit(pid, timeout - 50)
    else
      :ok
    end
  end

  defp wait_for_process_exit(_pid, _timeout), do: :ok
end
