defmodule KafkaEx do
  @moduledoc """
  KafkaEx Application and Worker Management

  This module handles application lifecycle and worker management for KafkaEx.
  For Kafka operations (produce, fetch, etc.), use `KafkaEx.API`.

  ## Usage

  Start a client for Kafka operations:

      {:ok, client} = KafkaEx.API.start_client()
      {:ok, offset} = KafkaEx.API.produce_one(client, "topic", 0, "message")

  Or use the `KafkaEx.API` behaviour in your own module:

      defmodule MyApp.Kafka do
        use KafkaEx.API, client: MyApp.KafkaClient
      end

      MyApp.Kafka.produce_one("topic", 0, "message")

  ## Worker-based API (Legacy)

  You can also create named workers under the KafkaEx supervisor:

      {:ok, pid} = KafkaEx.create_worker(:my_worker)
      {:ok, offset} = KafkaEx.API.produce_one(:my_worker, "topic", 0, "message")
  """

  use Application

  alias KafkaEx.Config

  @type uri() :: [{binary | [char], number}]
  @type worker_init :: [worker_setting]
  @type ssl_options :: [
          {:cacertfile, binary}
          | {:certfile, binary}
          | {:keyfile, binary}
          | {:password, binary}
        ]
  @type worker_setting ::
          {:uris, uri}
          | {:consumer_group, binary | :no_consumer_group}
          | {:metadata_update_interval, non_neg_integer}
          | {:consumer_group_update_interval, non_neg_integer}
          | {:ssl_options, ssl_options}
          | {:auth, KafkaEx.Auth.Config.t() | nil}
          | {:initial_topics, [binary]}

  @doc """
  Creates a KafkaEx worker under the application supervisor.

  ## Options

  - `consumer_group`: Name of the consumer group, `:no_consumer_group` for none
  - `uris`: List of brokers as `{"host", port}` tuples
  - `metadata_update_interval`: Metadata refresh interval in ms (default: 30000)
  - `consumer_group_update_interval`: Consumer group refresh interval in ms (default: 30000)
  - `use_ssl`: Enable SSL connections (default: false)
  - `ssl_options`: SSL options (see Erlang ssl docs)
  - `auth`: SASL authentication config (see `KafkaEx.Auth.Config`)

  ## Example

      {:ok, pid} = KafkaEx.create_worker(:my_worker)
      {:ok, pid} = KafkaEx.create_worker(:my_worker, uris: [{"localhost", 9092}])
  """
  @spec create_worker(atom, KafkaEx.worker_init()) :: Supervisor.on_start_child()
  def create_worker(name, worker_init \\ []) do
    server_impl = Config.server_impl()

    case build_worker_options(worker_init) do
      {:ok, worker_init} -> KafkaEx.Supervisor.start_child(server_impl, [worker_init, name])
      {:error, error} -> {:error, error}
    end
  end

  @doc """
  Stops a worker created with `create_worker/2`.

  Returns `:ok` on success or `{:error, reason}` on failure.
  """
  @spec stop_worker(atom | pid) :: :ok | {:error, :not_found} | {:error, :simple_one_for_one}
  def stop_worker(worker) do
    KafkaEx.Supervisor.stop_child(worker)
  end

  @doc """
  Returns the consumer group name for the given worker.

  Worker may be an atom or pid. Uses the default worker if not specified.
  """
  @spec consumer_group(atom | pid) :: binary | :no_consumer_group
  def consumer_group(worker \\ Config.default_worker()) do
    GenServer.call(worker, :consumer_group)
  end

  @doc """
  Starts and links a worker outside of a supervision tree.

  Takes the same arguments as `create_worker/2` plus:

  - `server_impl` - The GenServer module for the client (default: `KafkaEx.Client`)
  """
  @spec start_link_worker(atom, [KafkaEx.worker_setting() | {:server_impl, module}]) :: GenServer.on_start()
  def start_link_worker(name, worker_init \\ []) do
    {server_impl, worker_init} = Keyword.pop(worker_init, :server_impl, Config.server_impl())
    {:ok, full_worker_init} = build_worker_options(worker_init)
    server_impl.start_link(full_worker_init, name)
  end

  @doc """
  Builds worker options by merging with application config defaults.

  Returns `{:error, :invalid_consumer_group}` if consumer group is invalid.
  """
  @spec build_worker_options(worker_init) :: {:ok, worker_init} | {:error, :invalid_consumer_group}
  def build_worker_options(worker_init) do
    defaults = [
      uris: Config.brokers(),
      consumer_group: Config.default_consumer_group(),
      use_ssl: Config.use_ssl(),
      ssl_options: Config.ssl_options(),
      auth: Config.auth_config()
    ]

    worker_init = Keyword.merge(defaults, worker_init)
    supplied_consumer_group = Keyword.get(worker_init, :consumer_group)

    if valid_consumer_group?(supplied_consumer_group) do
      {:ok, worker_init}
    else
      {:error, :invalid_consumer_group}
    end
  end

  @doc """
  Returns true if the input is a valid consumer group or `:no_consumer_group`.
  """
  @spec valid_consumer_group?(any) :: boolean
  def valid_consumer_group?(:no_consumer_group), do: true
  def valid_consumer_group?(b) when is_binary(b), do: byte_size(b) > 0
  def valid_consumer_group?(_), do: false

  # OTP Application callback
  @doc false
  def start(_type, _args) do
    max_restarts = Application.get_env(:kafka_ex, :max_restarts, 10)
    max_seconds = Application.get_env(:kafka_ex, :max_seconds, 60)
    {:ok, pid} = KafkaEx.Supervisor.start_link(max_restarts, max_seconds)

    if Config.disable_default_worker() do
      {:ok, pid}
    else
      case KafkaEx.create_worker(Config.default_worker(), []) do
        {:error, reason} -> {:error, reason}
        {:ok, _} -> {:ok, pid}
      end
    end
  end
end
