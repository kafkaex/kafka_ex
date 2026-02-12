import Config

config :ex_unit, capture_log: is_nil(System.get_env("SHOW_LOGS"))

config :kafka_ex,
  disable_default_worker: true

config :kayrock, snappy_module: :snappyer

config :kafka_ex, sync_timeout: 60_000

# Help debug tests that are tricky to understand
config :logger, :console,
  format: "$time [$level] [$metadata] $message\n",
  metadata: [:module, :function, :pid]

config :aws_credentials,
  credential_providers: []
