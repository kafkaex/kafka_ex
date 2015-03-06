Kafka
========

Kafka client for Elixir/Erlang.

Usage
-----

```elixir
# In your config/config.exs
config Kafka,
  brokers: [{HOST, PORT}]

# Retrieve kafka metadata
Kafka.Server.metadata

# Retrieve the latest offset
Kafka.Server.latest_offset(topic, partition, name \\ Kafka.Server)

# Retrieve the earliest offset
Kafka.Server.earliest_offset(topic, partition, name \\ Kafka.Server)

# Retrieve offset from a specific time
Kafka.Server.offset(topic, partition, time, name \\ Kafka.Server) # time is of type [datetime](http://erlang.org/doc/man/calendar.html#type-datetime)

# Fetch kafka logs
Kafka.Server.fetch(topic, partition, offset, name \\ Kafka.Server, wait_time \\ 10, min_bytes \\ 1, max_bytes \\ 1_000_000)

# Produce kafka logs
Kafka.Server.produce(topic, partition, value, name \\ Kafka.Server, key \\ nil, required_acks \\ 0, timeout \\ 100)

# Stream kafka logs
Kafka.Server.stream(topic, partition, name \\ Kafka.Server, offset \\ 0, handler \\ KafkaHandler)
```


### Test
#### Unit tests
```
mix test --no-start
```

#### Integration tests 
Add the broker config to `config/config.exs` and run:
```
mix test --only integration
```
