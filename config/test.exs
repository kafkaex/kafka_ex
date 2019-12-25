use Mix.Config

config :kafka_ex, snappy_module: :snappyer

config :ex_unit, capture_log: true

config :kafka_ex,
  sync_timeout: 60_000
