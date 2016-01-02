use Mix.Config
  config :kafka_ex,
    brokers: [
      {"localhost", 9102},
      {"localhost", 9103},
      {"localhost", 9104},
    ],
    consumer_group: false
