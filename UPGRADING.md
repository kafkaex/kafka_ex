# Upgrading to KafkaEx 1.0

## Overview

KafkaEx 1.0 brings a cleaner API, removes legacy code, and uses Kayrock as the sole protocol implementation. 
This guide helps you migrate from KafkaEx 0.x.

## Breaking Changes

### Removed Legacy Servers

The following server implementations have been removed:

- `KafkaEx.Server0P8P0`
- `KafkaEx.Server0P8P2`
- `KafkaEx.Server0P9P0`
- `KafkaEx.Server0P10AndLater`

Kayrock is now the only implementation, providing automatic API version negotiation.

### Configuration Changes

**Removed options:**
- `kafka_version` - No longer needed; the client automatically negotiates versions

**Update your config:**
```elixir
# Before (0.x)
config :kafka_ex,
  kafka_version: "kayrock",
  brokers: [{"localhost", 9092}]

# After (1.0)
config :kafka_ex,
  brokers: [{"localhost", 9092}]
```

### Module Reorganization

Modules have been reorganized by domain:

| Old Module               | New Module                       |
|--------------------------|----------------------------------|
| `KafkaEx.GenConsumer`    | `KafkaEx.Consumer.GenConsumer`   |
| `KafkaEx.ConsumerGroup`  | `KafkaEx.Consumer.ConsumerGroup` |
| `KafkaEx.New.Client`     | `KafkaEx.Client`                 |
| `KafkaEx.New.KafkaExAPI` | `KafkaEx.API`                    |
| `KafkaEx.New.Kafka.*`    | `KafkaEx.Messages.*`             |

### API Changes

**New explicit client API:**
```elixir
# Before (0.x) - implicit worker
KafkaEx.produce("topic", 0, "message")
KafkaEx.fetch("topic", 0, offset: 0)

# After (1.0) - explicit client
{:ok, client} = KafkaEx.API.start_client(brokers: [{"localhost", 9092}])
{:ok, _} = KafkaEx.API.produce(client, "topic", 0, [%{value: "message"}])
{:ok, result} = KafkaEx.API.fetch(client, "topic", 0, 0)
```

### GenConsumer Changes

```elixir
# Before (0.x)
defmodule MyConsumer do
  use KafkaEx.GenConsumer
  # ...
end

# After (1.0)
defmodule MyConsumer do
  use KafkaEx.Consumer.GenConsumer
  # ...
end
```

### ConsumerGroup Changes

```elixir
# Before (0.x)
KafkaEx.ConsumerGroup.start_link(
MyConsumer, "my-group", ["topic"],
  # ...
)

# After (1.0)
KafkaEx.Consumer.ConsumerGroup.start_link(
  MyConsumer, "my-group", ["topic"],
  # ...
)
```

## Migration Checklist

- [ ] Remove `kafka_version` from config
- [ ] Update `KafkaEx.GenConsumer` to `KafkaEx.Consumer.GenConsumer`
- [ ] Update `KafkaEx.ConsumerGroup` to `KafkaEx.Consumer.ConsumerGroup`
- [ ] Update code to use `KafkaEx.API` functions (optional but recommended)
- [ ] Update any references to `KafkaEx.New.*` modules
- [ ] Run tests and fix deprecation warnings
- [ ] Verify with your Kafka cluster

## New Features in 1.0

### Explicit Client API

The new `KafkaEx.API` module provides explicit, client-based functions:

```elixir
{:ok, client} = KafkaEx.API.start_client(brokers: [{"localhost", 9092}])

# Produce
{:ok, metadata} = KafkaEx.API.produce_one(client, "topic", 0, "value")

# Fetch
{:ok, result} = KafkaEx.API.fetch(client, "topic", 0, 0)

# Offsets
{:ok, offset} = KafkaEx.API.latest_offset(client, "topic", 0)
{:ok, _} = KafkaEx.API.commit_offset(client, "group", "topic", [%{partition_num: 0, offset: offset}])

# Topic management
{:ok, _} = KafkaEx.API.create_topic(client, "new-topic", num_partitions: 3)
```

### Automatic API Version Negotiation

No need to configure Kafka versions - the client automatically negotiates the best API version with your brokers.

### Protocol Version Support

KafkaEx 1.0 supports modern protocol versions across all 15 Kafka APIs, with automatic
version negotiation. No configuration needed — the client selects the highest mutually
supported version with your brokers.

### Telemetry

KafkaEx now emits [telemetry](https://hexdocs.pm/telemetry/) events for observability
across connections, requests, produce, fetch, offsets, consumer groups, metadata, and
SASL authentication. See `KafkaEx.Telemetry` for the full event list.

### Structured Error Handling

All API functions return consistent `{:ok, result} | {:error, %KafkaEx.Client.Error{}}` tuples.
No more bare atoms, nil returns, or process crashes on broker errors.

### SASL Authentication

Full SASL support including PLAIN, SCRAM-SHA-256/512, OAUTHBEARER, and MSK_IAM:

```elixir
config :kafka_ex,
  brokers: [{"localhost", 9292}],
  use_ssl: true,
  sasl: %{
    mechanism: :scram,
    username: "user",
    password: "pass",
    mechanism_opts: %{algo: :sha256}
  }
```

## Getting Help

- GitHub Issues: https://github.com/kafkaex/kafka_ex/issues
- Slack: #kafkaex on elixir-lang.slack.com
