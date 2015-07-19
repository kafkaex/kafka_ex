use Mix.Config
  config :kafka_ex,
    brokers: [
      {"192.168.59.104", 49156}
    ],
    consumer_group: "kafka_ex"
