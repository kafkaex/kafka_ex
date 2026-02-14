KafkaEx
========

[![Unit Tests](https://github.com/kafkaex/kafka_ex/actions/workflows/unit-tests.yml/badge.svg)](https://github.com/kafkaex/kafka_ex/actions/workflows/unit-tests.yml)
[![Static Checks](https://github.com/kafkaex/kafka_ex/actions/workflows/static-checks.yml/badge.svg)](https://github.com/kafkaex/kafka_ex/actions/workflows/static-checks.yml)
[![Integration Tests](https://github.com/kafkaex/kafka_ex/actions/workflows/integration-tests.yml/badge.svg)](https://github.com/kafkaex/kafka_ex/actions/workflows/integration-tests.yml)
[![Coverage Status](https://coveralls.io/repos/github/kafkaex/kafka_ex/badge.svg?branch=master)](https://coveralls.io/github/kafkaex/kafka_ex?branch=master)
[![Hex.pm version](https://img.shields.io/hexpm/v/kafka_ex.svg?style=flat-square)](https://hex.pm/packages/kafka_ex)
[![Hex.pm downloads](https://img.shields.io/hexpm/dt/kafka_ex.svg?style=flat-square)](https://hex.pm/packages/kafka_ex)
[![License](https://img.shields.io/hexpm/l/kafka_ex.svg?style=flat-square)](https://hex.pm/packages/kafka_ex)
[![API Docs](https://img.shields.io/badge/api-docs-yellow.svg?style=flat)](http://hexdocs.pm/kafka_ex/)

KafkaEx is an Elixir client for [Apache Kafka](http://kafka.apache.org/) with
support for Kafka versions 0.10.0 and newer. KafkaEx requires Elixir 1.14+ and
Erlang OTP 24+.

See [http://hexdocs.pm/kafka_ex/](http://hexdocs.pm/kafka_ex/) for
documentation,
 [https://github.com/kafkaex/kafka_ex/](https://github.com/kafkaex/kafka_ex/)
 for code.

KafkaEx supports the following Kafka features:

*   Broker and Topic Metadata
*   Produce Messages
*   Fetch Messages
*   Message Compression with Snappy and gzip
*   Offset Management (fetch / commit / autocommit)
*   Consumer Groups
*   Topics Management (create / delete)

See [Kafka Protocol Documentation](http://kafka.apache.org/protocol.html) and
 [A Guide to the Kafka Protocol](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol)
for details of these features.

## KafkaEx v1.0

KafkaEx v1.0 uses [Kayrock](https://github.com/dantswain/kayrock) for Kafka
protocol serialization, providing automatic API version negotiation with your
Kafka brokers. This enables features like:

*   Message timestamps and headers
*   Offset storage in Kafka (not Zookeeper)
*   Support for modern Kafka protocol versions

The client automatically negotiates the appropriate API versions with your
Kafka cluster, so no version configuration is needed.

### New API: `KafkaEx.API`

The `KafkaEx.API` module provides a cleaner API with explicit client arguments:

```elixir
# Start a client
{:ok, client} = KafkaEx.API.start_client(brokers: [{"localhost", 9092}])

# Produce a message
{:ok, metadata} = KafkaEx.API.produce_one(client, "my-topic", 0, "hello")

# Fetch messages
{:ok, result} = KafkaEx.API.fetch(client, "my-topic", 0, 0)
```

You can also use it as a behaviour in your own modules:

```elixir
defmodule MyApp.Kafka do
  use KafkaEx.API, client: MyApp.KafkaClient
end

# Call without passing client:
MyApp.Kafka.produce_one("my-topic", 0, "hello")
```

For detailed information, see [KafkaEx.API](https://hexdocs.pm/kafka_ex/KafkaEx.API.html) on HexDocs.

## Using KafkaEx in an Elixir project

The standard approach for adding dependencies to an Elixir application applies:
add KafkaEx to the deps list in your project's mix.exs file.
You may also optionally add
[snappyer](https://hex.pm/packages/snappyer) (required
only if you want to use snappy compression).

```elixir
# mix.exs
defmodule MyApp.Mixfile do
  # ...

  defp deps do
    [
      # add to your existing deps
      {:kafka_ex, "~> 1.0"},
      # If using snappy-erlang-nif (snappy) compression
      {:snappy, git: "https://github.com/fdmanana/snappy-erlang-nif"}
      # if using snappyer (snappy) compression
      {:snappyer, "~> 1.2"}
    ]
  end
end
```

Then run `mix deps.get` to fetch dependencies.

## Configuration

See [config/config.exs](https://github.com/kafkaex/kafka_ex/blob/master/config/config.exs)
or [KafkaEx.Config](https://hexdocs.pm/kafka_ex/KafkaEx.Config.html)
for a description of configuration variables, including the Kafka broker list
and default consumer group.

## Telemetry

KafkaEx emits [telemetry](https://hexdocs.pm/telemetry/) events for observability. 
These events enable monitoring of connections, requests, consumers, and more.

### Event Categories

| Category   | Events   | Description                                                |
|------------|----------|------------------------------------------------------------|
| Connection | 4        | Connect, disconnect, reconnect, close                      |
| Request    | 4        | Request start/stop/exception, retry                        |
| Produce    | 4        | Produce start/stop/exception, batch metrics                |
| Fetch      | 4        | Fetch start/stop/exception, messages received              |
| Offset     | 4        | Commit/fetch offset operations                             |
| Consumer   | 8        | Group join, sync, heartbeat, rebalance, message processing |
| Metadata   | 4        | Cluster metadata updates                                   |
| SASL Auth  | 6        | PLAIN/SCRAM authentication spans                           |

### Example: Attaching a Handler

```elixir
defmodule MyApp.KafkaTelemetry do
  require Logger

  def attach do
    :telemetry.attach_many(
      "my-kafka-handler",
      [
        [:kafka_ex, :connection, :connect],
        [:kafka_ex, :request, :stop],
        [:kafka_ex, :produce, :stop],
        [:kafka_ex, :fetch, :stop]
      ],
      &handle_event/4,
      nil
    )
  end

  def handle_event([:kafka_ex, :connection, :connect], measurements, metadata, _config) do
    Logger.info("Connected to #{metadata.host}:#{metadata.port}")
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

For the complete list of events and their metadata, see [KafkaEx.Telemetry](https://hexdocs.pm/kafka_ex/KafkaEx.Telemetry.html).

## Error Handling and Resilience

KafkaEx v1.0 includes significant improvements to error handling and retry logic:

### Automatic Retries with Exponential Backoff

*   **Producer requests** - Automatically retry on leadership-related errors (`not_leader_for_partition`, `leader_not_available`) with metadata refresh
*   **Offset commits** - Retry transient errors (timeout, coordinator not available) with exponential backoff
*   **API version negotiation** - Retry parse errors during initial connection

### Consumer Group Resilience

Consumer groups handle transient errors gracefully following the Java client pattern (KAFKA-6829):

*   `unknown_topic_or_partition` triggers retry instead of crash
*   Heartbeat errors trigger rejoin for recoverable errors
*   Exponential backoff for join retries (1s→10s, up to 6 attempts)

### Safe Produce Retry Policy

**Important**: Produce requests only retry on leadership errors where we know the message wasn't written. Timeout errors are NOT retried to prevent potential duplicate messages. For truly idempotent produces, enable `enable.idempotence=true` on your Kafka cluster (requires Kafka 0.11+).

## Usage Examples

### Starting a Client

Start a client to interact with Kafka:

```elixir
# With default configuration (reads from config.exs)
{:ok, client} = KafkaEx.API.start_client()

# With custom brokers
{:ok, client} = KafkaEx.API.start_client(brokers: [{"localhost", 9092}])

# As a named process
{:ok, client} = KafkaEx.API.start_client(name: MyApp.KafkaClient)
```

Or add a client to your supervision tree:

```elixir
children = [
  {KafkaEx.API, name: MyApp.KafkaClient, brokers: [{"localhost", 9092}]}
]

Supervisor.start_link(children, strategy: :one_for_one)
```

You can also create named workers under the KafkaEx application supervisor:

```elixir
{:ok, pid} = KafkaEx.create_worker(:my_worker)
{:ok, pid} = KafkaEx.create_worker(:my_worker, uris: [{"localhost", 9092}])
```

### Produce Messages

```elixir
# Produce a single message
{:ok, metadata} = KafkaEx.API.produce_one(client, "my-topic", 0, "hello")

# Produce a batch of messages with keys
messages = [
  %{key: "user-1", value: "event-a"},
  %{key: "user-2", value: "event-b"}
]
{:ok, metadata} = KafkaEx.API.produce(client, "my-topic", 0, messages)

# metadata contains offset information
metadata.base_offset  # => 0
metadata.topic        # => "my-topic"
```

### Fetch Messages

```elixir
{:ok, result} = KafkaEx.API.fetch(client, "my-topic", 0, 0)

# result is a %KafkaEx.Messages.Fetch{} struct
result.topic          # => "my-topic"
result.partition      # => 0
result.high_watermark # => 10
result.records        # => [%{offset: 0, key: "user-1", value: "event-a"}, ...]

# Get the next offset to fetch from
next = KafkaEx.Messages.Fetch.next_offset(result)
{:ok, more} = KafkaEx.API.fetch(client, "my-topic", 0, next)
```

### Retrieve Offsets

```elixir
# Get the latest offset (end of partition)
{:ok, offset} = KafkaEx.API.latest_offset(client, "my-topic", 0)
# => {:ok, 42}

# Get the earliest offset (start of partition)
{:ok, offset} = KafkaEx.API.earliest_offset(client, "my-topic", 0)
# => {:ok, 0}
```

### Retrieve Metadata

```elixir
# Get cluster metadata (brokers + all topics)
{:ok, metadata} = KafkaEx.API.metadata(client)
metadata.brokers  # => %{0 => %{host: "localhost", port: 9092, ...}, ...}
metadata.topics   # => %{"my-topic" => %{name: "my-topic", partitions: [...]}, ...}

# Get metadata for specific topics
{:ok, topics} = KafkaEx.API.topics_metadata(client, ["my-topic"])
```

### Topic Management

```elixir
# Create a topic
{:ok, result} = KafkaEx.API.create_topic(client, "new-topic", num_partitions: 3)

# Delete a topic
{:ok, result} = KafkaEx.API.delete_topic(client, "old-topic")
```

### Consumer Groups

To use a consumer group, first implement a handler module using
`KafkaEx.Consumer.GenConsumer`.

```elixir
defmodule ExampleGenConsumer do
  use KafkaEx.Consumer.GenConsumer

  require Logger

  # note - messages are delivered in batches
  def handle_message_set(message_set, state) do
    for %{value: message} <- message_set do
      Logger.debug(fn -> "message: " <> inspect(message) end)
    end
    {:async_commit, state}
  end
end
```

Then add a `KafkaEx.Consumer.ConsumerGroup` to your application's supervision
tree and configure it to use the implementation module.

See the `KafkaEx.Consumer.GenConsumer` and `KafkaEx.Consumer.ConsumerGroup` documentation for
details.

### Compression

KafkaEx supports gzip, snappy, lz4, and zstd compression via the Kayrock protocol layer.
Decompression is handled automatically when fetching messages.

To produce compressed messages:

```elixir
{:ok, _} = KafkaEx.API.produce(client, "topic", 0, messages, compression: :gzip)
{:ok, _} = KafkaEx.API.produce(client, "topic", 0, messages, compression: :snappy)
{:ok, _} = KafkaEx.API.produce(client, "topic", 0, messages, compression: :lz4)
{:ok, _} = KafkaEx.API.produce(client, "topic", 0, messages, compression: :zstd)
```

Codec dependencies (gzip requires no extra deps):

| Codec | Dependency | Notes |
|-------|-----------|-------|
| gzip | Built-in (`:zlib`) | Always available |
| snappy | `{:snappyer, "~> 1.2"}` | Optional dep in mix.exs |
| lz4 | `{:lz4b, "~> 0.2.0"}` | Add to your mix.exs |
| zstd | `{:ezstd, "~> 1.0"}` | Requires Kafka 2.1+ and Produce API v7+ |

## SASL Authentication

KafkaEx supports connecting to secure Kafka clusters with SASL mechanisms.

Example:

```elixir
# config/config.exs
config :kafka_ex,
  brokers: [{"localhost", 9292}],
  use_ssl: true,
  ssl_options: [verify: :verify_none],
  sasl: %{
    mechanism: :scram,
    username: System.get_env("KAFKA_USERNAME"),
    password: System.get_env("KAFKA_PASSWORD"),
    mechanism_opts: %{algo: :sha256}  # or :sha512
  }
```

Or via worker options:

```elixir
{:ok, _pid} = KafkaEx.create_worker(:sasl_worker, [
  uris: [{"localhost", 9292}],
  use_ssl: true,
  ssl_options: [verify: :verify_none],
  auth: KafkaEx.Auth.Config.new(%{
    mechanism: :plain,
    username: "alice",
    password: "secret123"
  })
])
```

> ✅ Use SSL/TLS with PLAIN (never send passwords in cleartext).  
> ✅ Prefer SCRAM over PLAIN when supported.  
> ✅ PLAIN requires Kafka 0.9.0+, SCRAM requires 0.10.2+.  

👉 See [AUTH.md](./AUTH.md) for full details, configuration examples, and troubleshooting tips.

## Testing

It is strongly recommended to test using the Dockerized test cluster described
below.  This is required for contributions to KafkaEx.

**NOTE** You may have to run the test suite twice to get tests to pass.  Due to
asynchronous issues, the test suite sometimes fails on the first try.

### Dockerized Test Cluster

Testing KafkaEx requires a local SSL-enabled Kafka cluster with 3 nodes: one
node listening on appropriate port. The easiest way to do this
is using the scripts in
this repository that utilize [Docker](https://www.docker.com) and
[Docker Compose](https://www.docker.com/products/docker-compose) (both of which
are freely available).  This is the method we use for our CI testing of
KafkaEx.

Ports:
9092-9094 - No authentication (SSL)
9192-9194 - SASL/PLAIN (SSL)
9292-9294 - SASL/SCRAM (SSL)
9392-9394 - SASL/OAUTHBEARER (SSL)

To launch the included test cluster, run

```
./scripts/docker_up.sh
```

The `docker_up.sh` script will attempt to determine an IP address for your
computer on an active network interface.

The test cluster runs Kafka 2.8+.

### Running the KafkaEx Tests

Tests are organized by tag. Integration test tags (`:consume`, `:produce`, `:lifecycle`, `:consumer_group`, `:auth`, `:chaos`) 
are excluded by default so unit tests run without a Kafka cluster.

#### Unit tests

No Kafka cluster required:

```bash
mix test.unit
```

#### Integration tests

Start the Docker test cluster first, then run by category:

```bash
./scripts/docker_up.sh

# Run a specific category
mix test --only consumer_group
mix test --only produce
mix test --only consume
mix test --only lifecycle
mix test --only auth

# Or run all integration tests
mix test.integration
```

#### Chaos tests

Chaos tests use Testcontainers and do not require the Docker Compose cluster:

```bash
mix test --only chaos
```

### Static analysis

```bash
mix format --check-formatted
mix credo --strict
mix dialyzer
```

## Contributing

All contributions are managed through the
[kafkaex github repo](https://github.com/kafkaex/kafka_ex).

If you find a bug or would like to contribute, please open an
[issue](https://github.com/kafkaex/kafka_ex/issues) or submit a pull
request.  Please refer to [CONTRIBUTING.md](CONTRIBUTING.md) for our
contribution process.

KafkaEx has a Slack channel: #kafkaex on
[elixir-lang.slack.com](http://elixir-lang.slack.com). You can request
an invite via [http://bit.ly/slackelixir](http://bit.ly/slackelixir).
The Slack channel is appropriate for quick questions or general design
discussions.  The Slack discussion is archived at
[http://slack.elixirhq.com/kafkaex](http://slack.elixirhq.com/kafkaex).

## Snappy Module

KafkaEx uses [snappyer](https://hex.pm/packages/snappyer) for snappy compression.
Add it to your deps if you need snappy support — it is an optional dependency.
