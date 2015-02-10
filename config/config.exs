use Mix.Config

if Mix.env == :test do
  config Kafka,
    brokers: [{System.get_env("HOST"), System.get_env("PORT") || 9092}]
end
