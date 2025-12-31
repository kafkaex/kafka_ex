defmodule KafkaEx.Config do
  @moduledoc """
  Configuration module for KafkaEx.

  ## Configuration Options

  Add to your `config/config.exs`:

      config :kafka_ex,
        # Required: List of broker addresses
        brokers: [{"localhost", 9092}],

        # Connection settings
        use_ssl: false,
        ssl_options: [],
        # sasl: %{mechanism: :plain, username: "user", password: "pass"},

        # Client settings
        client_id: "kafka_ex",
        sync_timeout: 3000,

        # Consumer group (used for offset storage)
        default_consumer_group: "kafka_ex",

        # Partitioner for produce requests (when partition is nil)
        partitioner: KafkaEx.Producer.Partitioner.Default,

        # Application settings
        disable_default_worker: false,
        max_restarts: 10,
        max_seconds: 60,

        # GenConsumer settings
        commit_interval: 5_000,
        commit_threshold: 100,
        auto_offset_reset: :none,

        # Compression
        snappy_module: :snappyer

  ## Broker Configuration

  Brokers can be configured in several formats:

      # List of tuples
      brokers: [{"host1", 9092}, {"host2", 9092}]

      # CSV string
      brokers: "host1:9092,host2:9092"

      # Dynamic (MFA or function)
      brokers: {MyModule, :get_brokers, []}
      brokers: fn -> [...] end

  ## SASL Authentication

      config :kafka_ex,
        use_ssl: true,  # Required for SASL
        sasl: %{
          mechanism: :scram,  # :plain or :scram
          username: System.get_env("KAFKA_USER"),
          password: System.get_env("KAFKA_PASS"),
          mechanism_opts: %{algorithm: :sha256}  # For SCRAM
        }
  """

  alias KafkaEx.Auth.Config, as: AuthConfig

  require Logger

  @doc """
  Returns true if default worker should not be started on application boot.
  """
  @spec disable_default_worker() :: boolean()
  def disable_default_worker do
    Application.get_env(:kafka_ex, :disable_default_worker, false)
  end

  @doc """
  Returns the configured client ID.
  """
  @spec client_id() :: String.t()
  def client_id do
    Application.get_env(:kafka_ex, :client_id, "kafka_ex")
  end

  @doc """
  Returns the default consumer group name.

  Checks `:default_consumer_group` first, falls back to `:consumer_group` for
  backward compatibility.
  """
  @spec default_consumer_group() :: String.t() | :no_consumer_group
  def default_consumer_group do
    Application.get_env(:kafka_ex, :default_consumer_group) ||
      Application.get_env(:kafka_ex, :consumer_group, "kafka_ex")
  end

  @doc """
  Returns the default consumer group name.

  Deprecated: Use `default_consumer_group/0` instead.
  """
  @deprecated "Use default_consumer_group/0 instead"
  @spec consumer_group() :: String.t() | :no_consumer_group
  def consumer_group do
    default_consumer_group()
  end

  @doc """
  Returns the configured partitioner module.

  The partitioner is used when producing messages without specifying a partition.
  Defaults to `KafkaEx.Producer.Partitioner.Default`.
  """
  @spec partitioner() :: module()
  def partitioner do
    Application.get_env(:kafka_ex, :partitioner, KafkaEx.Producer.Partitioner.Default)
  end

  @doc """
  Returns true if SSL is enabled.
  """
  @spec use_ssl() :: boolean()
  def use_ssl, do: Application.get_env(:kafka_ex, :use_ssl, false)

  @doc """
  Returns SSL options for connections.

  Validates that ssl_options is a keyword list when SSL is enabled.
  """
  @spec ssl_options() :: Keyword.t()
  def ssl_options do
    ssl_options(use_ssl(), Application.get_env(:kafka_ex, :ssl_options, []))
  end

  @doc """
  Returns the default worker name.
  """
  @spec default_worker() :: atom()
  def default_worker do
    :kafka_ex
  end

  @doc """
  Returns the server implementation module.

  Always returns `KafkaEx.Client` in v1.0+.
  """
  @spec server_impl() :: module()
  def server_impl do
    KafkaEx.Client
  end

  @doc """
  Returns the configured broker list.

  Supports multiple formats:
  - List of tuples: `[{"host", port}]`
  - CSV string: `"host:port,host:port"`
  - MFA tuple: `{module, function, args}`
  - Zero-arity function
  """
  @spec brokers() :: [{String.t(), pos_integer()}] | nil
  def brokers do
    :kafka_ex
    |> Application.get_env(:brokers)
    |> normalize_brokers()
  end

  @doc """
  Returns the authentication configuration.
  """
  @spec auth_config() :: AuthConfig.t() | nil
  def auth_config do
    AuthConfig.from_env()
  end

  # Broker normalization helpers

  defp normalize_brokers(nil), do: nil

  defp normalize_brokers(csv) when is_binary(csv) do
    for line <- String.split(csv, ","), into: [] do
      case line |> String.trim() |> String.split(":") do
        [host] ->
          msg = "Port not set for Kafka broker #{host}"
          Logger.warning(msg)
          raise msg

        [host, port] ->
          {port, _} = Integer.parse(port)
          {host, port}
      end
    end
  end

  defp normalize_brokers(list) when is_list(list), do: list
  defp normalize_brokers({mod, fun, args}) when is_atom(mod) and is_atom(fun), do: apply(mod, fun, args)
  defp normalize_brokers(fun) when is_function(fun, 0), do: fun.()

  # SSL options validation

  defp ssl_options(false, []), do: []

  defp ssl_options(false, options) do
    Logger.warning(
      "Ignoring ssl_options #{inspect(options)} because " <>
        "use_ssl is false. If you do not intend to use ssl and want to " <>
        "remove this warning, set `ssl_options: []` in the KafkaEx config."
    )

    []
  end

  defp ssl_options(true, options) do
    if Keyword.keyword?(options) do
      options
    else
      raise(ArgumentError, "SSL is enabled and invalid ssl_options were provided: #{inspect(options)}")
    end
  end
end
