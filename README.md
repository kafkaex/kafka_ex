Kafka_Ex
========

Kafka client for Elixir/Erlang.

Usage
-----

```elixir
# In your config/config.exs
config Kafka,
  brokers: [{HOST, PORT}]

# Retrieve kafka metadata
Kakfa.Server.metadata

# Retrieve the latest offset
Kakfa.Server.latest_offset(topic, partition)

# Retrieve the earliest offset
Kafka.Server.earliest_offset(topic, partition)

# Retrieve offset from a specific time
Kafka.Server.offset(topic, partition, time) # time is of type [datetime](http://erlang.org/doc/man/calendar.html#type-datetime)

# Fetch kafka logs
Kafka.Server.fetch(topic, partition, offset, wait_time \\ 10, min_bytes \\ 1, max_bytes \\ 1_000_000)

# Produce kafka logs
Kafka.Server.produce(topic, partition, value, key \\ nil, required_acks \\ 0, timeout \\ 100)
```


### Test
Run unit tests with:
`mix test --no-start`

Rub integration tests with:
HOST=kafka_host PORT=kafka_port mix test --integration
