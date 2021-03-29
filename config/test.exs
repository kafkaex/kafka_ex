use Mix.Config

config :ex_unit, capture_log: is_nil(System.get_env("SHOW_LOGS"))

config :kafka_ex, snappy_module: :snappy

config :kafka_ex, sync_timeout: 60_000

# Help debug tests that are tricky to understand
config :logger, :console,
  format: "$time [$level] [$metadata] $message\n",
  metadata: [:module, :function, :pid]
