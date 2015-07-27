use Mix.Config
  config :kafka_ex,
    brokers: [
      {"localhost", 9092}
    ],
    consumer_group: false
