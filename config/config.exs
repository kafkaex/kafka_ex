use Mix.Config
  config :kafka_ex,
    brokers: [
      {"localhost", 9092},
      {"localhost", 9093},
      {"localhost", 9094},
    ],
    consumer_group: false
