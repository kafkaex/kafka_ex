import Config

config :kafka_ex,
  # A list of brokers to connect to. This can be in either of the following formats
  #
  #  * [{"HOST", port}...]
  #  * CSV - `"HOST:PORT,HOST:PORT[,...]"`
  #  * {mod, fun, args}
  #  * &arity_zero_fun/0
  #  * fn -> ... end
  #
  # If you receive :leader_not_available
  # errors when producing messages, it may be necessary to modify "advertised.host.name" in the
  # server.properties file.
  # In the case below you would set "advertised.host.name=localhost"
  brokers: [
    {"localhost", 9092},
    {"localhost", 9093},
    {"localhost", 9094}
  ],
  #
  # OR:
  # brokers: "localhost:9092,localhost:9093,localhost:9094"
  #
  # It may be useful to configure your brokers at runtime, for example if you use
  # service discovery instead of storing your broker hostnames in a config file.
  # To do this, you can use `{mod, fun, args}` or a zero-arity function, and `KafkaEx`
  # will invoke your callback when fetching the `:brokers` configuration.
  # Note that when using this approach you must return a list of host/port pairs.
  #
  # the default consumer group for worker processes, must be a binary (string)
  #    NOTE if you are on Kafka < 0.8.2 or if you want to disable the use of
  #    consumer groups, set this to :no_consumer_group (this is the
  #    only exception to the requirement that this value be a binary)
  consumer_group: "kafka_ex",
  # The client_id is the logical grouping of a set of kafka clients.
  client_id: "kafka_ex",
  # Set this value to true if you do not want the default
  # `KafkaEx.Server` worker to start during application start-up -
  # i.e., if you want to start your own set of named workers
  disable_default_worker: false,
  # Timeout value, in msec, for synchronous operations (e.g., network calls).
  # If this value is greater than GenServer's default timeout of 5000, it will also
  # be used as the timeout for work dispatched via KafkaEx.Server.call (e.g., KafkaEx.metadata).
  # In those cases, it should be considered a 'total timeout', encompassing both network calls and
  # wait time for the genservers.
  sync_timeout: 3000,
  # Supervision max_restarts - the maximum amount of restarts allowed in a time frame
  max_restarts: 10,
  # Supervision max_seconds -  the time frame in which :max_restarts applies
  max_seconds: 60,
  # Interval in milliseconds that GenConsumer waits to commit offsets.
  commit_interval: 5_000,
  # Threshold number of messages consumed for GenConsumer to commit offsets
  # to the broker.
  commit_threshold: 100,
  # The policy for resetting offsets when an :offset_out_of_range error occurs
  # Options:
  # - `:earliest` - Will move to the offset to the oldest available
  # - `:latest` - Will move the offset to the most recent.
  # - `:none` - The error will simply be raised
  auto_offset_reset: :none,
  # Interval in milliseconds to wait before reconnect to kafka
  sleep_for_reconnect: 400,
  # This is the flag that enables use of ssl
  use_ssl: true,
  # see SSL OPTION DESCRIPTIONS - CLIENT SIDE at http://erlang.org/doc/man/ssl.html
  # for supported options
  ssl_options: [
    # Fix warnings. More at https://github.com/erlang/otp/issues/5352
    verify: :verify_none,
    cacertfile: File.cwd!() <> "/ssl/ca-cert",
    certfile: File.cwd!() <> "/ssl/cert.pem",
    keyfile: File.cwd!() <> "/ssl/key.pem"
  ],
  snappy_module: :snappyer,
  # set this to the version of the kafka broker that you are using
  # include only major.minor.patch versions.  must be at least 0.8.0
  # use "kayrock" for the new client
  kafka_version: "0.10.1"

env_config = Path.expand("#{Mix.env()}.exs", __DIR__)

if File.exists?(env_config) do
  import_config(env_config)
end
