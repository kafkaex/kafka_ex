use Mix.Config
  config KafkaEx,
    brokers: [
      {"192.168.59.103", 49154}
    ],
    consumer_group: "kafka_ex"
