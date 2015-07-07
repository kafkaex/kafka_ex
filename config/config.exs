use Mix.Config
  config :kafka_ex,
    brokers: [
      {"192.168.59.103", 49154}
    ],
    consumer_group: "kafka_ex"
