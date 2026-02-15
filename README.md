KafkaEx
========

[![Unit Tests](https://github.com/kafkaex/kafka_ex/actions/workflows/unit-tests.yml/badge.svg)](https://github.com/kafkaex/kafka_ex/actions/workflows/unit-tests.yml)
[![Integration Tests](https://github.com/kafkaex/kafka_ex/actions/workflows/integration-tests.yml/badge.svg)](https://github.com/kafkaex/kafka_ex/actions/workflows/integration-tests.yml)
[![Static Checks](https://github.com/kafkaex/kafka_ex/actions/workflows/static-checks.yml/badge.svg)](https://github.com/kafkaex/kafka_ex/actions/workflows/static-checks.yml)
[![Coverage Status](https://coveralls.io/repos/github/kafkaex/kafka_ex/badge.svg?branch=master)](https://coveralls.io/github/kafkaex/kafka_ex?branch=master)
[![Hex.pm version](https://img.shields.io/hexpm/v/kafka_ex.svg?style=flat-square)](https://hex.pm/packages/kafka_ex)
[![Hex.pm downloads](https://img.shields.io/hexpm/dt/kafka_ex.svg?style=flat-square)](https://hex.pm/packages/kafka_ex)
[![License](https://img.shields.io/hexpm/l/kafka_ex.svg?style=flat-square)](https://hex.pm/packages/kafka_ex)
[![API Docs](https://img.shields.io/badge/api-docs-yellow.svg?style=flat-square)](http://hexdocs.pm/kafka_ex/)

KafkaEx is an Elixir client for [Apache Kafka](http://kafka.apache.org/) with support for Kafka versions 0.10.0 and newer. KafkaEx requires Elixir 1.14+ and Erlang OTP 24+.

## 📚 Documentation

- **HexDocs:** [http://hexdocs.pm/kafka_ex/](http://hexdocs.pm/kafka_ex/)
- **GitHub:** [https://github.com/kafkaex/kafka_ex/](https://github.com/kafkaex/kafka_ex/)
- **Authentication:** [AUTH.md](./AUTH.md)
- **Contributing:** [CONTRIBUTING.md](./CONTRIBUTING.md)

## Table of Contents

- [Features](#features)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Usage](#usage)
  - [Basic Producer/Consumer](#basic-producerconsumer)
  - [Consumer Groups](#consumer-groups)
  - [Metadata & Offsets](#metadata--offsets)
  - [Topic Management](#topic-management)
  - [Compression](#compression)
- [Authentication (SASL)](#authentication-sasl)
- [Telemetry & Observability](#telemetry--observability)
- [Error Handling & Resilience](#error-handling--resilience)
- [Testing](#testing)
- [Contributing](#contributing)

## Features

KafkaEx v1.0 uses [Kayrock](https://github.com/dantswain/kayrock) for Kafka protocol serialization with **automatic API version negotiation**—no manual version configuration needed.

### Core Capabilities

- ✅ **Producer** - Single and batch message production with timestamps and headers
- ✅ **Consumer** - Message fetching with offset management
- ✅ **Consumer Groups** - Coordinated consumption with automatic partition assignment
- ✅ **Compression** - Gzip, Snappy, LZ4, and Zstd
- ✅ **Authentication** - SASL/PLAIN, SASL/SCRAM, OAUTHBEARER, AWS MSK IAM
- ✅ **SSL/TLS** - Secure connections with certificate-based authentication
- ✅ **Topic Management** - Create and delete topics programmatically
- ✅ **Metadata API** - Discover brokers, topics, and partitions
- ✅ **Offset Management** - Commit, fetch, and reset offsets
- ✅ **Telemetry** - Built-in observability with telemetry events
- ✅ **Automatic Retries** - Smart retry logic with exponential backoff

### Supported Kafka Versions

- **Minimum:** Kafka 0.10.0+
- **Recommended:** Kafka 0.11.0+ (for RecordBatch format, headers, timestamps)
- **Tested with:** Kafka 2.1.0 through 3.x

## Quick Start

### Installation

Add KafkaEx to your `mix.exs` dependencies:

```elixir
def deps do
  [
    # For release candidate:
    {:kafka_ex, "~> 1.0.0-rc.1"}
    # For stable release (when available):
    # {:kafka_ex, "~> 1.0"}
  ]
end
```

Then run:
```bash
mix deps.get
```

### Simple Producer & Consumer

```elixir
# Start a client
{:ok, client} = KafkaEx.API.start_client(brokers: [{"localhost", 9092}])

# Produce a message
{:ok, _metadata} = KafkaEx.API.produce_one(client, "my-topic", 0, "hello")

# Fetch messages from offset 0
{:ok, result} = KafkaEx.API.fetch(client, "my-topic", 0, 0)
```

### Using KafkaEx.API as a Behaviour

For production applications, define a module with the KafkaEx.API behaviour:

```elixir
defmodule MyApp.Kafka do
  use KafkaEx.API, client: MyApp.KafkaClient
end

# In your application.ex supervision tree:
children = [
  {KafkaEx.Client, name: MyApp.KafkaClient, brokers: [{"localhost", 9092}]}
]

# Now call without passing client:
MyApp.Kafka.produce_one("my-topic", 0, "hello")
{:ok, messages} = MyApp.Kafka.fetch("my-topic", 0, 0)
```

See [KafkaEx.API documentation](https://hexdocs.pm/kafka_ex/KafkaEx.API.html) for the complete API reference.

## Configuration

KafkaEx can be configured via `config.exs` or by passing options directly to `KafkaEx.API.start_client/1`.

### Basic Configuration

```elixir
# config/config.exs
config :kafka_ex,
  # List of Kafka brokers
  brokers: [{"localhost", 9092}, {"localhost", 9093}],

  # Client identifier
  client_id: "my-app",

  # Default consumer group
  default_consumer_group: "my-consumer-group",

  # Request timeout (milliseconds)
  sync_timeout: 10_000
```

### SSL/TLS Configuration

```elixir
config :kafka_ex,
  brokers: [{"kafka.example.com", 9093}],
  use_ssl: true,
  ssl_options: [
    cacertfile: "/path/to/ca-cert.pem",
    certfile: "/path/to/client-cert.pem",
    keyfile: "/path/to/client-key.pem",
    verify: :verify_peer
  ]
```

### Consumer Group Settings

```elixir
config :kafka_ex,
  default_consumer_group: "my-group",

  # Metadata refresh interval (milliseconds)
  consumer_group_update_interval: 30_000,

  # Auto-commit settings
  commit_interval: 5_000,      # Commit every 5 seconds
  commit_threshold: 100,       # Or every 100 messages

  # What to do when no offset exists
  auto_offset_reset: :earliest # or :latest
```

### Compression Configuration

Compression is set per-request, not globally:

```elixir
# Produce with gzip compression
KafkaEx.API.produce(client, "topic", 0, messages, compression: :gzip)

# Supported: :none (default), :gzip, :snappy, :lz4, :zstd
```

For Snappy compression, add to mix.exs:
```elixir
{:snappyer, "~> 1.2"}
```

### Dynamic Configuration

You can use MFA or anonymous functions for dynamic broker resolution:

```elixir
# Using MFA tuple
config :kafka_ex,
  brokers: {MyApp.Config, :get_kafka_brokers, []}

# Using anonymous function
config :kafka_ex,
  brokers: fn -> Application.get_env(:my_app, :kafka_brokers) end
```

See [KafkaEx.Config](https://hexdocs.pm/kafka_ex/KafkaEx.Config.html) for all available options.

## Usage

### Basic Producer/Consumer

#### Producing Messages

```elixir
# Single message
{:ok, metadata} = KafkaEx.API.produce_one(
  client,
  "my-topic",
  0,                    # partition
  "hello world"         # message value
)

# With message key (for partition routing)
{:ok, metadata} = KafkaEx.API.produce_one(
  client,
  "my-topic",
  0,
  "message value",
  key: "user-123"
)

# Batch produce
messages = [
  %{value: "message 1", key: "key1"},
  %{value: "message 2", key: "key2"},
  %{value: "message 3", key: "key3"}
]

{:ok, metadata} = KafkaEx.API.produce(client, "my-topic", 0, messages)
```

#### Consuming Messages

```elixir
# Fetch from specific offset
{:ok, result} = KafkaEx.API.fetch(client, "my-topic", 0, 100)

result.records
|> Enum.each(fn record ->
  IO.puts("Offset: #{record.offset}, Value: #{record.value}")
end)

# Fetch all messages (earliest to high watermark)
{:ok, result} = KafkaEx.API.fetch_all(client, "my-topic", 0)
```

### Consumer Groups

Consumer groups provide coordinated consumption with automatic partition assignment and offset management.

#### 1. Implement a Consumer

```elixir
defmodule MyApp.MessageConsumer do
  use KafkaEx.Consumer.GenConsumer
  require Logger

  # Messages are delivered in batches
  def handle_message_set(message_set, state) do
    Enum.each(message_set, fn record ->
      Logger.info("Processing: #{inspect(record.value)}")
      # Process your message here
    end)

    # Commit offsets asynchronously
    {:async_commit, state}
  end
end
```

Available commit strategies:
- `{:async_commit, state}` - Commit in background (recommended)
- `{:sync_commit, state}` - Wait for commit to complete

#### 2. Add to Supervision Tree

```elixir
# In your application.ex
def start(_type, _args) do
  children = [
    # Start the consumer group
    %{
      id: MyApp.MessageConsumer,
      start: {
        KafkaEx.Consumer.ConsumerGroup,
        :start_link,
        [
          MyApp.MessageConsumer,   # Your consumer module
          "my-consumer-group",      # Consumer group ID
          ["topic1", "topic2"],     # Topics to consume
          [
            # Optional configuration
            commit_interval: 5_000,
            commit_threshold: 100,
            auto_offset_reset: :earliest
          ]
        ]
      }
    }
  ]

  Supervisor.start_link(children, strategy: :one_for_one)
end
```

See [KafkaEx.Consumer.GenConsumer](https://hexdocs.pm/kafka_ex/KafkaEx.Consumer.GenConsumer.html) for details.

### Metadata & Offsets

#### Topic Metadata

```elixir
# Get all topics
{:ok, metadata} = KafkaEx.API.metadata(client)

# Get specific topics
{:ok, metadata} = KafkaEx.API.metadata(client, ["topic1", "topic2"])

# Inspect partitions
metadata.topics
|> Enum.each(fn topic ->
  IO.puts("Topic: #{topic.name}, Partitions: #{length(topic.partitions)}")
end)
```

#### Offset Operations

```elixir
# Get latest offset for a partition
{:ok, offset} = KafkaEx.API.latest_offset(client, "my-topic", 0)

# Get earliest offset
{:ok, offset} = KafkaEx.API.earliest_offset(client, "my-topic", 0)

# List offsets by timestamp
timestamp = DateTime.utc_now() |> DateTime.add(-3600, :second) |> DateTime.to_unix(:millisecond)
partition_request = %{partition_num: 0, timestamp: timestamp}
{:ok, offsets} = KafkaEx.API.list_offsets(client, [{"my-topic", [partition_request]}])

# Fetch committed offset for consumer group
{:ok, offset} = KafkaEx.API.fetch_committed_offset(
  client,
  "my-consumer-group",
  "my-topic",
  0
)

# Commit offset for consumer group
partitions = [%{partition_num: 0, offset: 100}]
{:ok, result} = KafkaEx.API.commit_offset(
  client,
  "my-consumer-group",
  "my-topic",
  partitions
)
```

### Topic Management

```elixir
# Create a topic
{:ok, result} = KafkaEx.API.create_topic(
  client,
  "new-topic",
  num_partitions: 3,
  replication_factor: 2,
  config_entries: %{
    "retention.ms" => "86400000",
    "compression.type" => "gzip"
  }
)

# Delete a topic
{:ok, result} = KafkaEx.API.delete_topic(client, "old-topic")
```

### Compression

KafkaEx supports multiple compression formats. Compression is applied per-request:

```elixir
# Gzip compression (built-in)
{:ok, _} = KafkaEx.API.produce(
  client,
  "my-topic",
  0,
  messages,
  compression: :gzip
)

# Snappy compression (requires snappyer package)
{:ok, _} = KafkaEx.API.produce(
  client,
  "my-topic",
  0,
  messages,
  compression: :snappy
)

# LZ4 compression (built-in, Kafka 0.9.0+)
{:ok, _} = KafkaEx.API.produce(
  client,
  "my-topic",
  0,
  messages,
  compression: :lz4
)

# Zstd compression (built-in, Kafka 2.1.0+)
{:ok, _} = KafkaEx.API.produce(
  client,
  "my-topic",
  0,
  messages,
  compression: :zstd
)
```

**Supported Formats:**

| Format | Kafka Version | Dependency Required |
|--------|---------------|---------------------|
| `:gzip` | 0.7.0+ | None (built-in) |
| `:snappy` | 0.8.0+ | `{:snappyer, "~> 1.2"}` |
| `:lz4` | 0.9.0+ | None (built-in) |
| `:zstd` | 2.1.0+ | None (built-in) |

Decompression is handled automatically when consuming messages.

## Authentication (SASL)

KafkaEx supports multiple SASL authentication mechanisms for secure connections to Kafka clusters.

### SASL/PLAIN

Simple username/password authentication. **Always use with SSL/TLS** to protect credentials.

```elixir
config :kafka_ex,
  brokers: [{"kafka.example.com", 9092}],
  use_ssl: true,
  ssl_options: [verify: :verify_peer, cacertfile: "/path/to/ca.pem"],
  sasl: %{
    mechanism: :plain,
    username: "alice",
    password: "secret123"
  }
```

### SASL/SCRAM

Challenge-response authentication (more secure than PLAIN).

```elixir
config :kafka_ex,
  brokers: [{"kafka.example.com", 9092}],
  use_ssl: true,
  sasl: %{
    mechanism: :scram,
    username: "alice",
    password: "secret123",
    mechanism_opts: %{algo: :sha256}  # or :sha512
  }
```

### OAUTHBEARER

OAuth 2.0 token-based authentication.

```elixir
config :kafka_ex,
  brokers: [{"kafka.example.com", 9092}],
  use_ssl: true,
  sasl: %{
    mechanism: :oauthbearer,
    mechanism_opts: %{
      token_provider: &MyApp.get_oauth_token/0,
      extensions: %{"traceId" => "optional-data"}
    }
  }
```

### AWS MSK IAM

AWS IAM authentication for Amazon Managed Streaming for Kafka (MSK).

```elixir
config :kafka_ex,
  brokers: [{"msk-cluster.region.amazonaws.com", 9098}],
  use_ssl: true,
  sasl: %{
    mechanism: :msk_iam,
    mechanism_opts: %{
      region: "us-east-1"
      # Credentials automatically resolved from environment
    }
  }
```

**Authentication Requirements:**

| Mechanism   | Minimum Kafka | SSL Required   | Notes                           |
|-------------|---------------|----------------|---------------------------------|
| PLAIN       | 0.9.0+        | ✅ Yes          | Never use without SSL/TLS       |
| SCRAM       | 0.10.2+       | ⚠️ Recommended | Challenge-response, more secure |
| OAUTHBEARER | 2.0+          | ⚠️ Recommended | Requires token provider         |
| MSK_IAM     | MSK 2.7.1+    | ✅ Yes          | AWS-specific                    |

See [AUTH.md](./AUTH.md) for detailed authentication setup and troubleshooting.

## Telemetry & Observability

KafkaEx emits [telemetry](https://hexdocs.pm/telemetry/) events for monitoring connections, requests, and consumer operations.

### Event Categories

| Category   | Events | Description                                                |
|------------|--------|------------------------------------------------------------|
| Connection | 4      | Connect, disconnect, reconnect, close                      |
| Request    | 4      | Request start/stop/exception, retry                        |
| Produce    | 4      | Produce start/stop/exception, batch metrics                |
| Fetch      | 4      | Fetch start/stop/exception, messages received              |
| Offset     | 4      | Commit/fetch offset operations                             |
| Consumer   | 8      | Group join, sync, heartbeat, rebalance, message processing |
| Metadata   | 4      | Cluster metadata updates                                   |
| SASL Auth  | 6      | PLAIN/SCRAM authentication spans                           |

### Example: Attaching a Handler

```elixir
defmodule MyApp.KafkaTelemetry do
  require Logger

  def attach do
    :telemetry.attach_many(
      "my-kafka-handler",
      [
        [:kafka_ex, :connection, :stop],
        [:kafka_ex, :request, :stop],
        [:kafka_ex, :produce, :stop],
        [:kafka_ex, :fetch, :stop]
      ],
      &handle_event/4,
      nil
    )
  end

  def handle_event([:kafka_ex, :connection, :stop], measurements, metadata, _config) do
    Logger.info("Connected to #{metadata.host}:#{metadata.port} in #{measurements.duration / 1_000_000}ms")
  end

  def handle_event([:kafka_ex, :request, :stop], measurements, metadata, _config) do
    Logger.debug("Request #{metadata.api_key} took #{measurements.duration / 1_000_000}ms")
  end

  def handle_event([:kafka_ex, :produce, :stop], measurements, metadata, _config) do
    Logger.info("Produced #{measurements.message_count} messages to #{metadata.topic}")
  end

  def handle_event([:kafka_ex, :fetch, :stop], measurements, metadata, _config) do
    Logger.info("Fetched #{measurements.message_count} messages from #{metadata.topic}")
  end
end
```

Then in your application startup:

```elixir
# application.ex
def start(_type, _args) do
  MyApp.KafkaTelemetry.attach()
  # ...
end
```

See [KafkaEx.Telemetry](https://hexdocs.pm/kafka_ex/KafkaEx.Telemetry.html) for the complete event reference.

## Error Handling & Resilience

KafkaEx v1.0 includes smart error handling and retry logic for production resilience.

### Automatic Retries with Exponential Backoff

- **Producer requests** - Automatically retry on leadership-related errors (`not_leader_for_partition`, `leader_not_available`) with metadata refresh
- **Offset commits** - Retry transient errors (timeout, coordinator not available) with exponential backoff
- **API version negotiation** - Retry parse errors during initial connection

### Consumer Group Resilience

Consumer groups handle transient errors gracefully following the Java client pattern (KAFKA-6829):

- `unknown_topic_or_partition` triggers retry instead of crash
- Heartbeat errors trigger rejoin for recoverable errors
- Exponential backoff for join retries (1s→10s, up to 6 attempts)

### Safe Produce Retry Policy

**Important**: Produce requests only retry on leadership errors where we know the message wasn't written. Timeout errors are **NOT retried** to prevent potential duplicate messages.

For truly idempotent produces, enable `enable.idempotence=true` on your Kafka cluster (requires Kafka 0.11+).

### SSL/TLS Timeouts (Known Issue)

When using certain versions of OTP, [random timeouts can occur with SSL](https://github.com/kafkaex/kafka_ex/issues/389).

**Impacted versions:**
- OTP 21.3.8.1 → 21.3.8.14
- OTP 22.1 → 22.3.1

**Solution:** Upgrade to OTP 21.3.8.15 or 22.3.2+.

## Testing

### Unit Tests (No Kafka Required)

Run tests that don't require a live Kafka cluster:

```bash
mix test.unit
```

### Integration Tests (Docker Required)

KafkaEx includes a Dockerized test cluster with 3 Kafka brokers configured with different authentication mechanisms:

**Ports:**
- 9092-9094: No authentication (SSL)
- 9192-9194: SASL/PLAIN (SSL)
- 9292-9294: SASL/SCRAM (SSL)
- 9392-9394: SASL/OAUTHBEARER (SSL)

**Start the test cluster:**

```bash
./scripts/docker_up.sh
```

**Run all tests:**

```bash
# Unit tests
mix test.unit

# Integration tests
mix test.integration

# All tests together
mix test
```

**Run specific test categories:**

```bash
mix test --only consumer_group
mix test --only produce
mix test --only consume
mix test --only auth
```

**Run SASL tests:**

```bash
MIX_ENV=test mix test --include sasl
```

### Static Analysis

```bash
mix format              # Format code
mix format --check-formatted
mix credo --strict      # Linting
mix dialyzer            # Type checking
```

## Contributing

All contributions are managed through the [KafkaEx GitHub repo](https://github.com/kafkaex/kafka_ex).

- **Issues:** [github.com/kafkaex/kafka_ex/issues](https://github.com/kafkaex/kafka_ex/issues)
- **Pull Requests:** See [CONTRIBUTING.md](CONTRIBUTING.md) for our contribution process
- **Slack:** #kafkaex on [elixir-lang.slack.com](http://elixir-lang.slack.com) ([request invite](http://bit.ly/slackelixir))
- **Slack Archive:** [slack.elixirhq.com/kafkaex](http://slack.elixirhq.com/kafkaex)

## License

KafkaEx is released under the MIT License. See [LICENSE](LICENSE) for details.
