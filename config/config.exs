use Mix.Config

defmodule ConfigHelper do
  def kafka_host do
    System.get_env("KAFKA_HOST") || "localhost"
  end
end

config :kafka_ex,
  # a list of brokers to connect to in {"HOST", port} format
  brokers: [
    {ConfigHelper.kafka_host(), 9092},
    {ConfigHelper.kafka_host(), 9093},
    {ConfigHelper.kafka_host(), 9094},
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
  # Timeout value, in msec, for synchronous operations (e.g., network calls)
  sync_timeout: 3000,
  # Supervision max_restarts - the maximum amount of restarts allowed in a time frame
  max_restarts: 10,
  # Supervision max_seconds -  the time frame in which :max_restarts applies
  max_seconds: 60,
  # This is the flag that enables use of ssl
  use_ssl: false,
  # see SSL OPTION DESCRIPTIONS - CLIENT SIDE at http://erlang.org/doc/man/ssl.html
  # for supported options
  ssl_options: [
    cacertfile: System.cwd <> "/ssl/ca-cert",
    certfile: System.cwd <> "/ssl/cert.pem",
    keyfile: System.cwd <> "/ssl/key.pem",
  ],
  kafka_version: "0.9.0"

env_config = Path.expand("#{Mix.env}.exs", __DIR__)
if File.exists?(env_config) do
  import_config(env_config)
end
