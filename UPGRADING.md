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

### Headers API — `[%Header{}]` instead of `[{key, value}]`

The `headers:` option on every produce function now takes a list of
`%KafkaEx.Messages.Header{}` structs instead of `{key, value}` tuples.
This is a **runtime breaking change** — your code will compile and
only fail with `FunctionClauseError` on the first produce. Migrate
before upgrading in production.

```elixir
# Before (0.x / rc.2)
KafkaEx.API.produce(client, "t", 0, [
  %{value: "v", headers: [{"trace-id", "abc"}, {"tenant", "prod"}]}
])

# After (1.0)
alias KafkaEx.Messages.Header
KafkaEx.API.produce(client, "t", 0, [
  %{value: "v", headers: [
    Header.new("trace-id", "abc"),
    Header.new("tenant", "prod")
  ]}
])
```

Why: the fetch path was already returning `%Header{}` structs. The
produce side was the asymmetric outlier; a single consistent shape
across produce and fetch makes round-trip code cleaner.

### Broker version requirements

- **Minimum: Kafka 0.11.0+** — required for RecordBatch format,
  headers, and timestamps. Earlier brokers will fail at produce.
- **Tested: Kafka 2.1.0 through 3.8.x.**
- **Kafka 2.3+ recommended** — needed for KIP-394 two-step
  JoinGroup semantics with `group.initial.rebalance.delay.ms`.
  kafka_ex auto-handles the two-step dance, but broker support is
  required.
- **Kafka 4.0+** — partial compatibility; tracked in
  [#497](https://github.com/kafkaex/kafka_ex/issues/497). Consumer
  groups may hit protocol changes.

### Optional dependency matrix

Some features require additional deps in your app's `mix.exs`. If
you configure a feature without the backing dep, you'll get an
`UndefinedFunctionError` at runtime (not at startup).

| Feature | Required dep |
|---|---|
| Snappy compression | `{:snappyer, "~> 1.2"}` |
| Zstd compression | `{:ezstd, "~> 1.0"}` |
| LZ4 compression | `{:lz4b, "~> 0.0.13"}` |
| MSK-IAM SASL | `{:jason, "~> 1.0"}`, `{:aws_signature, "~> 0.4"}`, `{:aws_credentials, "~> 1.0"}` |
| OAuth JWT parsing | user's choice (e.g., `{:joken, "~> 2.6"}` — only if your token_provider needs to parse JWTs) |

### 0.x → 1.0 API cheat-sheet

| 0.x | 1.0 |
|---|---|
| `KafkaEx.produce("t", 0, "m")` | `KafkaEx.API.produce_one(client, "t", 0, "m")` |
| `KafkaEx.fetch("t", 0, offset: 0)` | `KafkaEx.API.fetch(client, "t", 0, 0)` |
| `KafkaEx.GenConsumer` | `KafkaEx.Consumer.GenConsumer` |
| `KafkaEx.ConsumerGroup` | `KafkaEx.Consumer.ConsumerGroup` |
| `config :kafka_ex, kafka_version: "kayrock"` | (remove — no longer needed) |
| `headers: [{"k", "v"}]` on produce | `headers: [Header.new("k", "v")]` |

### OffsetCommit error handling (new in 1.0)

In earlier kafka_ex, `:illegal_generation` and related errors were
logged and swallowed — the consumer kept running on a stale
generation until the next heartbeat happened to also fail.

v1.0 classifies OffsetCommit errors across three paths, matching
the reference Kafka clients (Java, librdkafka, brod, kafka-python):

- **Terminal** (`:fenced_instance_id`, `:group_authorization_failed`,
  `:topic_authorization_failed`, `:offset_metadata_too_large`,
  `:invalid_commit_offset_size`) — consumer stops without rejoining.
  Under `restart: :transient` the supervisor does not respawn.
- **Fatal** (`:illegal_generation`, `:unknown_member_id`) — GenConsumer
  casts `{:rejoin_required, reason, stale_gen}` to the group manager
  and self-stops. The manager resets member_id/generation_id and
  runs a rebalance. Duplicate casts from sibling partitions coalesce
  in the manager's mailbox.
- **Retryable** (`:rebalance_in_progress`, `:unstable_offset_commit`,
  `:timeout`, `:coordinator_not_available`, …) — commit is retried
  with exponential backoff.

No user callback is invoked — kafka_ex v1 does not have a synchronous
`handle_commit_failure/3` behaviour (deferred post-1.0). Subscribe
to the new telemetry event to observe failures:

```elixir
:telemetry.attach(
  "my-commit-failure-observer",
  [:kafka_ex, :consumer, :commit_failed],
  fn _event, %{count: 1}, metadata, _ ->
    # metadata: %{group_id, topic, partition, offset, kind, error}
    Logger.warning("Commit failed: #{inspect(metadata)}")
  end,
  nil
)
```

At-least-once semantics are preserved: any uncommitted messages
since the last successful commit will be redelivered after the
rejoin, so your `handle_message_set/2` must be idempotent (or
tolerate duplicates).

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

## Deprecations

The following functions and modules are deprecated in v1.0 and scheduled for
removal in v2.0. They continue to work in the entire 1.x series — plan migration
at your convenience.

| Deprecated                                         | Replacement                                        | Notes                                       |
|----------------------------------------------------|----------------------------------------------------|---------------------------------------------|
| `KafkaEx.Config.consumer_group/0`                  | `KafkaEx.Config.default_consumer_group/0`          | Function-for-function swap.                 |
| `KafkaEx.Client.State.max_supported_api_version/3` | `KafkaEx.Client.State.max_supported_api_version/2` | Drop the default arg and match on `{:ok, vsn}` / `{:error, :api_not_supported_by_broker}`. |
| `KafkaEx.Producer.Partitioner.Legacy`              | `KafkaEx.Producer.Partitioner.Default`             | See `KafkaEx.Producer.Partitioner` moduledoc. |

Each of these emits an Elixir compile-time `@deprecated` warning — `mix compile --warnings-as-errors` will flag the first call site.

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
