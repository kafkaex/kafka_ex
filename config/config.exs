use Mix.Config

if Mix.env == :test do
  config Kafka,
    brokers: [{System.get_env("HOST"), "PORT" |> System.get_env |> String.to_integer}]
end
