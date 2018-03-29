use Mix.Config

config :kafka_ex,
  # A list of brokers to connect to in {"HOST", port} format. If you receive :leader_not_available
  # errors when producing messages, it may be necessary to modify "advertised.host.name" in the
  # server.properties file.
  # In the case below you would set "advertised.host.name=localhost"
  brokers: [
    {"localhost", 9092},
    {"localhost", 9093},
    {"localhost", 9094},
  ],
  # the default consumer group for worker processes, must be a binary (string)
  #    NOTE if you are on Kafka < 0.8.2 or if you want to disable the use of
  #    consumer groups, set this to :no_consumer_group (this is the
  #    only exception to the requirement that this value be a binary)
  consumer_group: "kafka_ex",
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
  # This is the flag that enables use of ssl
  use_ssl: true,
  # see SSL OPTION DESCRIPTIONS - CLIENT SIDE at http://erlang.org/doc/man/ssl.html
  # for supported options
  ssl_options: [
    cacertfile: System.cwd <> "/ssl/ca-cert",
    certfile: System.cwd <> "/ssl/cert.pem",
    keyfile: System.cwd <> "/ssl/key.pem",
  ],
  # set this to the version of the kafka broker that you are using
  # include only major.minor.patch versions.  must be at least 0.8.0
  kafka_version: "0.9.0"

env_config = Path.expand("#{Mix.env}.exs", __DIR__)
if File.exists?(env_config) do
  import_config(env_config)
end
