Kafka_Ex
========

Kafka client for Elixir/Erlang.

Usage
-----

```elixir
consumer = Kafka.Consumer.new(broker_list, client_id)
Kafka.Consumer.subscribe(consumer, ["test"], &handle_kafka/1)

def handle_kafka(data) do
  IO.inspect data
end
```

