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
KafkaEx.fetch("topic", 0, 0)  # offset is positional

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
- [ ] Update `KafkaEx.GenConsumer` to `KafkaEx.Consumer.GenConsumer` (required - code will not compile)
- [ ] Update `KafkaEx.ConsumerGroup` to `KafkaEx.Consumer.ConsumerGroup` (required - code will not compile)
- [ ] Update code to use `KafkaEx.API` functions (optional but recommended)
- [ ] Update any references to `KafkaEx.New.*` modules
- [ ] If you depend on specific protocol versions, add `api_versions` to config (see API Version Resolution below)
- [ ] Run tests and fix deprecation warnings
- [ ] Verify with your Kafka cluster

**Important:** Old module names (`KafkaEx.GenConsumer`, `KafkaEx.ConsumerGroup`, etc.) are **not aliased**. Code using old module names will fail to compile immediately. All references must be updated.

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

### API Version Resolution

The client now uses the highest protocol version supported by both the broker and the protocol library by default. Previous versions used conservative hardcoded defaults (e.g., fetch v3, produce v3) even when the broker supported higher versions.

If you need to pin specific API versions — for example, to match previous behavior or work around broker-specific issues — use the new `api_versions` application config:

```elixir
config :kafka_ex,
  api_versions: %{
    fetch: 3,
    produce: 3,
    metadata: 1
  }
```

Version selection follows this priority order:

1. Per-request `:api_version` option (highest priority)
2. Application config `api_versions` map
3. Broker-negotiated max (default)

The `GenConsumer` / `ConsumerGroup` `:api_versions` supervisor option continues to work for per-consumer-group overrides. Application config is no longer read by `GenConsumer` directly — it is handled centrally by the client's request builder.

`latest_offset/4` and `earliest_offset/4` no longer force list_offsets v1. They use the standard version resolution like all other API calls.

### Telemetry & Observability

Built-in telemetry support for monitoring connections, requests, and consumer operations:

```elixir
:telemetry.attach(
  "kafka-handler",
  [:kafka_ex, :request, :stop],
  &MyApp.handle_event/4,
  nil
)
```

See [README.md](./README.md#telemetry--observability) for complete event reference and setup examples.

### Compression Support

Support for multiple compression formats on a per-request basis:

```elixir
# Gzip compression (built-in)
{:ok, _} = KafkaEx.API.produce(client, "topic", 0, messages, compression: :gzip)

# Supported: :gzip, :snappy, :lz4, :zstd
```

See [README.md](./README.md#compression) for details on all compression formats.

### SASL Authentication

Full SASL support including PLAIN, SCRAM-SHA-256/512, OAUTHBEARER, and AWS MSK IAM:

```elixir
# SCRAM example
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

See [AUTH.md](./AUTH.md) for complete configuration examples for all authentication mechanisms.

## Getting Help

- GitHub Issues: https://github.com/kafkaex/kafka_ex/issues
- Slack: #kafkaex on elixir-lang.slack.com
