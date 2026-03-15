# KafkaEx Usage Rules for AI Agents

## Understanding KafkaEx

KafkaEx is an Elixir client for Apache Kafka with support for Kafka versions 0.10.0 and newer. Version 1.0 uses Kayrock for protocol serialization with automatic API version negotiation.

**Read the documentation before attempting to use KafkaEx.** Do not assume you know the API from prior knowledge of Kafka clients in other languages. The v1.0 API has specific patterns that differ from legacy versions.

## Core Principles

1. **Use `KafkaEx.API` for all new code** - This is the primary, modern interface
2. **Pass the client explicitly** - All API functions require a client parameter
3. **Offsets are positional** - Not keyword arguments in most functions
4. **Return types are tuples** - Functions return `{:ok, result}` or `{:error, reason}`
5. **Consumer groups are the pattern** - Use `GenConsumer` for coordinated consumption

## Critical API Patterns

### Starting a Client

```elixir
# CORRECT - Primary pattern
{:ok, client} = KafkaEx.API.start_client(brokers: [{"localhost", 9092}])

# CORRECT - In supervision tree
children = [
  {KafkaEx.Client, name: MyApp.KafkaClient, brokers: [{"localhost", 9092}]}
]

# CORRECT - Using as behaviour
defmodule MyApp.Kafka do
  use KafkaEx.API, client: MyApp.KafkaClient
end

# AVOID - Legacy worker API (backward compatibility only)
KafkaEx.create_worker(:worker_name, uris: [...])
```

### Producing Messages

```elixir
# CORRECT - Single message
{:ok, metadata} = KafkaEx.API.produce_one(client, "topic", 0, "message")

# CORRECT - With message key
{:ok, metadata} = KafkaEx.API.produce_one(client, "topic", 0, "value", key: "key1")

# CORRECT - Batch produce
messages = [%{value: "msg1", key: "k1"}, %{value: "msg2", key: "k2"}]
{:ok, metadata} = KafkaEx.API.produce(client, "topic", 0, messages)

# CORRECT - With compression
{:ok, metadata} = KafkaEx.API.produce(client, "topic", 0, messages, compression: :gzip)

# WRONG - Missing client parameter
KafkaEx.produce("topic", 0, "message")  # This is legacy API, don't use in v1.0 code
```

### Consuming Messages

```elixir
# CORRECT - Fetch with positional offset
{:ok, result} = KafkaEx.API.fetch(client, "topic", 0, 100)

# WRONG - Offset as keyword argument
KafkaEx.API.fetch(client, "topic", 0, offset: 100)  # Will fail!

# CORRECT - Fetch all messages
{:ok, result} = KafkaEx.API.fetch_all(client, "topic", 0)

# Access records
result.records |> Enum.each(fn record ->
  # record has: offset, key, value, timestamp, headers
end)
```

### Offset Management

```elixir
# CORRECT - Fetch committed offset (partitions is a LIST of maps)
partitions = [%{partition_num: 0}]
{:ok, offsets} = KafkaEx.API.fetch_committed_offset(client, "group", "topic", partitions)

# WRONG - Passing bare partition number
KafkaEx.API.fetch_committed_offset(client, "group", "topic", 0)  # Will fail!

# CORRECT - Commit offset (partitions is a LIST of maps)
partitions = [%{partition_num: 0, offset: 100}]
{:ok, result} = KafkaEx.API.commit_offset(client, "group", "topic", partitions)

# CORRECT - List offsets by timestamp
timestamp = DateTime.utc_now() |> DateTime.add(-3600, :second) |> DateTime.to_unix(:millisecond)
partition_request = %{partition_num: 0, timestamp: timestamp}
{:ok, offsets} = KafkaEx.API.list_offsets(client, [{"topic", [partition_request]}])
```

### Topic Management

```elixir
# CORRECT - Returns {:ok, result}, NOT :ok
{:ok, result} = KafkaEx.API.create_topic(
  client,
  "my-topic",
  num_partitions: 3,
  replication_factor: 2
)

# WRONG - Pattern matching for :ok atom
:ok = KafkaEx.API.create_topic(client, "topic")  # Will fail!

# CORRECT - Delete topic also returns tuple
{:ok, result} = KafkaEx.API.delete_topic(client, "old-topic")
```

## Consumer Groups - The Right Pattern

Consumer groups are the recommended pattern for coordinated consumption:

```elixir
# 1. Define a consumer implementing GenConsumer behaviour
defmodule MyApp.Consumer do
  use KafkaEx.Consumer.GenConsumer
  require Logger

  def handle_message_set(message_set, state) do
    Enum.each(message_set, fn record ->
      Logger.info("Processing: #{record.value}")
      # Your processing logic
    end)

    # CORRECT - Only two valid return types
    {:async_commit, state}  # Recommended
    # OR
    {:sync_commit, state}   # For critical offset tracking
  end
end

# 2. Add to supervision tree
children = [
  %{
    id: MyApp.Consumer,
    start: {
      KafkaEx.Consumer.ConsumerGroup,
      :start_link,
      [
        MyApp.Consumer,        # Your consumer module
        "my-consumer-group",   # Group ID
        ["topic1", "topic2"],  # Topics
        []                     # Options
      ]
    }
  }
]
```

**Note:** There is NO `{:no_commit, state}` return option. Only `{:async_commit, state}` and `{:sync_commit, state}` are valid.

## Authentication (SASL)

KafkaEx supports 5 SASL mechanisms:

### PLAIN (Requires SSL/TLS)

```elixir
config :kafka_ex,
  brokers: [{"localhost", 9192}],
  use_ssl: true,
  sasl: %{
    mechanism: :plain,
    username: "alice",
    password: "secret"
  }
```

**CRITICAL:** Always use SSL with PLAIN. The library validates this and returns `:plain_requires_tls` error without SSL.

### SCRAM (SHA-256 or SHA-512)

```elixir
config :kafka_ex,
  brokers: [{"localhost", 9292}],
  use_ssl: true,  # Recommended but not required
  sasl: %{
    mechanism: :scram,
    username: "alice",
    password: "secret",
    mechanism_opts: %{algo: :sha256}  # :sha256 (default) or :sha512
  }
```

### OAUTHBEARER

```elixir
config :kafka_ex,
  brokers: [{"localhost", 9392}],
  use_ssl: true,
  sasl: %{
    mechanism: :oauthbearer,
    mechanism_opts: %{
      token_provider: &MyApp.get_token/0,  # 0-arity function
      extensions: %{"key" => "value"}      # Optional
    }
  }
```

**Token Provider Requirements:**
- Must be 0-arity function
- Must return `{:ok, token}` or `{:error, reason}`
- Token must be non-empty binary string
- Called once per connection (implement caching if needed)

### AWS MSK IAM

```elixir
config :kafka_ex,
  brokers: [{"b-1.cluster.kafka.region.amazonaws.com", 9098}],
  use_ssl: true,
  sasl: %{
    mechanism: :msk_iam,
    mechanism_opts: %{
      region: "us-east-1"
      # Credentials auto-resolved from AWS environment
    }
  }
```

**Note:** Requires port 9098 for MSK IAM listener.

## Compression

Compression is per-request, not global config:

```elixir
# Supported formats: :gzip, :snappy, :lz4, :zstd
{:ok, _} = KafkaEx.API.produce(client, "topic", 0, messages, compression: :gzip)
```

**Dependencies:**
- `:gzip`, `:lz4`, `:zstd` - Built-in, no dependencies
- `:snappy` - Requires `{:snappyer, "~> 1.2"}` in mix.exs

## Common Mistakes to Avoid

### ❌ Using offset as keyword argument

```elixir
# WRONG
KafkaEx.API.fetch(client, "topic", 0, offset: 100)

# CORRECT
KafkaEx.API.fetch(client, "topic", 0, 100)
```

### ❌ Passing partition as integer instead of list

```elixir
# WRONG
KafkaEx.API.commit_offset(client, "group", "topic", 0, 100)

# CORRECT
partitions = [%{partition_num: 0, offset: 100}]
KafkaEx.API.commit_offset(client, "group", "topic", partitions)
```

### ❌ Pattern matching for :ok atom on create/delete

```elixir
# WRONG
:ok = KafkaEx.API.create_topic(client, "topic")

# CORRECT
{:ok, result} = KafkaEx.API.create_topic(client, "topic")
```

### ❌ Using legacy worker API in new code

```elixir
# WRONG for v1.0
KafkaEx.create_worker(:worker_name)
KafkaEx.produce("topic", 0, "message")

# CORRECT for v1.0
{:ok, client} = KafkaEx.API.start_client()
{:ok, _} = KafkaEx.API.produce_one(client, "topic", 0, "message")
```

### ❌ Invalid GenConsumer return types

```elixir
# WRONG
def handle_message_set(messages, state) do
  {:no_commit, state}  # This doesn't exist!
end

# CORRECT - Only two valid types
def handle_message_set(messages, state) do
  {:async_commit, state}  # Recommended
  # OR
  {:sync_commit, state}   # For critical cases
end
```

## Configuration Best Practices

1. **Always use SSL/TLS in production**
2. **Use environment variables for credentials** - Never hardcode
3. **SCRAM is preferred over PLAIN** when both available
4. **Set reasonable timeouts** - Default `sync_timeout` is 3000ms
5. **Configure consumer commit settings** - `commit_interval` (5000ms) and `commit_threshold` (100 messages)

## Testing Patterns

### Running Tests Locally

```bash
# Unit tests only (no Kafka)
mix test.unit

# Start Docker test cluster
./scripts/docker_up.sh

# Integration tests
mix test.integration

# Specific categories
mix test --only consumer_group
mix test --only produce
mix test --only auth
```

### Docker Test Ports

- 9092-9094: No authentication
- 9192-9194: SASL/PLAIN
- 9292-9294: SASL/SCRAM
- 9392-9394: SASL/OAUTHBEARER

## Error Handling

All KafkaEx.API functions return:
- `{:ok, result}` on success
- `{:error, error_atom}` on failure

Common error atoms:
- `:not_leader_for_partition` - Metadata refresh needed (automatic retry)
- `:unknown_topic_or_partition` - Topic doesn't exist
- `:offset_out_of_range` - Requested offset doesn't exist
- `:sasl_authentication_failed` - Invalid credentials
- `:plain_requires_tls` - PLAIN auth without SSL
- `:correlation_mismatch` - Protocol error (shouldn't happen)

## Module Organization

**v1.0 Module Structure:**
- `KafkaEx.API` - Primary API, use this for all operations
- `KafkaEx.Client` - Client implementation (used via API)
- `KafkaEx.Consumer.GenConsumer` - Consumer behaviour (NOT `KafkaEx.GenConsumer`)
- `KafkaEx.Consumer.ConsumerGroup` - Consumer group coordinator
- `KafkaEx.Messages.*` - Message structs (Fetch, Produce, etc.)
- `KafkaEx.Auth.Config` - Authentication configuration builder

**Removed in v1.0:**
- `KafkaEx.GenConsumer` → Moved to `KafkaEx.Consumer.GenConsumer`
- `KafkaEx.ConsumerGroup` → Moved to `KafkaEx.Consumer.ConsumerGroup`
- `KafkaEx.New.*` → Removed entirely (merged into main modules)
- Legacy server implementations (Server0P8P0, Server0P9P0, etc.)

## Telemetry Integration

KafkaEx emits telemetry events for observability:

```elixir
:telemetry.attach(
  "kafka-handler",
  [:kafka_ex, :request, :stop],
  &MyApp.handle_event/4,
  nil
)
```

**Key events:**
- `[:kafka_ex, :connection, :start]` - Connecting to broker
- `[:kafka_ex, :connection, :stop]` - Connection established
- `[:kafka_ex, :request, :stop]` - Request completed
- `[:kafka_ex, :produce, :stop]` - Produce completed
- `[:kafka_ex, :fetch, :stop]` - Fetch completed
- `[:kafka_ex, :consumer, :rebalance]` - Consumer group rebalanced

## Version Requirements

- **Minimum Kafka:** 0.10.0+
- **Recommended Kafka:** 0.11.0+ (for headers, timestamps, RecordBatch format)
- **Elixir:** 1.14+
- **Erlang OTP:** 24+

## When to Use What

**Use `produce_one/5`:**
- Single message production
- Simple use cases
- When you need immediate feedback per message

**Use `produce/5`:**
- Batch message production
- Higher throughput needed
- Multiple messages to same partition

**Use `fetch/5`:**
- Fetching from specific offset
- Manual offset management
- Low-level control

**Use `fetch_all/4`:**
- Fetching all available messages
- Processing backlog
- Catch-up scenarios

**Use Consumer Groups:**
- Coordinated consumption across multiple instances
- Automatic partition assignment
- Automatic offset management
- Production applications (recommended)

## Migration from 0.x

If upgrading from KafkaEx 0.x:
1. Read UPGRADING.md completely
2. Update all module names (GenConsumer, ConsumerGroup paths changed)
3. Remove `kafka_version` config
4. Update to use `KafkaEx.API` functions
5. Old modules are NOT aliased - code will fail to compile immediately

## Authentication Security

**CRITICAL Rules:**
- PLAIN requires `use_ssl: true` (enforced by library)
- SCRAM is cryptographically secure even without TLS (password never sent in cleartext)
- OAUTHBEARER token provider is called once per connection (implement caching if needed)
- MSK IAM requires port 9098, not standard Kafka ports

## Reference Documentation

- **Complete API:** README.md
- **Authentication:** AUTH.md (all 5 SASL mechanisms)
- **Migration:** UPGRADING.md (breaking changes and checklist)
- **Contributing:** CONTRIBUTING.md (testing, code quality)
- **HexDocs:** https://hexdocs.pm/kafka_ex/

## Quick Reference

```elixir
# Start client
{:ok, client} = KafkaEx.API.start_client(brokers: [{"localhost", 9092}])

# Produce
{:ok, _} = KafkaEx.API.produce_one(client, "topic", 0, "message", key: "key1")

# Consume
{:ok, result} = KafkaEx.API.fetch(client, "topic", 0, 0)

# Metadata
{:ok, metadata} = KafkaEx.API.metadata(client, ["topic1"])

# Offsets
{:ok, offset} = KafkaEx.API.latest_offset(client, "topic", 0)

# Topic management
{:ok, _} = KafkaEx.API.create_topic(client, "new-topic", num_partitions: 3)

# Consumer groups - use GenConsumer behaviour
defmodule MyConsumer do
  use KafkaEx.Consumer.GenConsumer

  def handle_message_set(messages, state) do
    # Process messages
    {:async_commit, state}
  end
end
```
