use Mix.Config

config :kafka_ex,
  # a list of brokers to connect to in {"HOST", port} format
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
  # Timeout value, in msec, for synchronous operations (e.g., network calls)
  sync_timeout: 3000,
  # Supervision Strategy
  # Modify this if you want to configure the strategy the supervisor will use
  # to restart the workers.
  # if you need help configuring the supervision strategy see:
  # http://elixir-lang.org/docs/stable/elixir/Supervisor.Spec.html#supervise/2
  # The default supervision strategy is as configured below:
  supervision_strategy: [
    strategy: :simple_one_for_one, max_restarts: 10, max_seconds: 60
  ]
