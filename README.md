KafkaEx
========

[![CI Tests](https://github.com/kafkaex/kafka_ex/actions/workflows/test.yml/badge.svg)](https://github.com/kafkaex/kafka_ex/actions/workflows/test.yml)
[![CI Checks](https://github.com/kafkaex/kafka_ex/actions/workflows/checks.yml/badge.svg)](https://github.com/kafkaex/kafka_ex/actions/workflows/checks.yml)
[![Coverage Status](https://coveralls.io/repos/github/kafkaex/kafka_ex/badge.svg?branch=master)](https://coveralls.io/github/kafkaex/kafka_ex?branch=master)
[![Hex.pm version](https://img.shields.io/hexpm/v/kafka_ex.svg?style=flat-square)](https://hex.pm/packages/kafka_ex)
[![Hex.pm downloads](https://img.shields.io/hexpm/dt/kafka_ex.svg?style=flat-square)](https://hex.pm/packages/kafka_ex)
[![License](https://img.shields.io/hexpm/l/kafka_ex.svg?style=flat-square)](https://hex.pm/packages/kafka_ex)
[![API Docs](https://img.shields.io/badge/api-docs-yellow.svg?style=flat)](http://hexdocs.pm/kafka_ex/)

KafkaEx is an Elixir client for [Apache Kafka](http://kafka.apache.org/) with
support for Kafka versions 0.10.0 and newer. KafkaEx requires Elixir 1.6+ and
Erlang OTP 19+.

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

For information on the new API available in the `KafkaEx.New` namespace, see:

*   Github: [new_api.md](https://github.com/kafkaex/kafka_ex/blob/master/new_api.md)
*   HexDocs: [New API](new_api.html)

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
      {:kafka_ex, "~> 0.11"},
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

You can also override options when creating a worker, see below.

## Timeouts with SSL

When using certain versions of OTP,
[random timeouts can occur if using SSL](https://github.com/kafkaex/kafka_ex/issues/389).

Impacted versions:

*   OTP 21.3.8.1 -> 21.3.8.14
*   OTP 22.1 -> 22.3.1

Upgrade respectively to 21.3.8.15 or 22.3.2 to solve this.

## Usage Examples

### Consumer Groups

To use a consumer group, first implement a handler module using
`KafkaEx.GenConsumer`.

```elixir
defmodule ExampleGenConsumer do
  use KafkaEx.GenConsumer

  alias KafkaEx.Protocol.Fetch.Message

  require Logger

  # note - messages are delivered in batches
  def handle_message_set(message_set, state) do
    for %Message{value: message} <- message_set do
      Logger.debug(fn -> "message: " <> inspect(message) end)
    end
    {:async_commit, state}
  end
end
```

Then add a `KafkaEx.ConsumerGroup` to your application's supervision
tree and configure it to use the implementation module.

See the `KafkaEx.GenConsumer` and `KafkaEx.ConsumerGroup` documentation for
details.

### Create a KafkaEx Worker

KafkaEx worker processes manage the state of the connection to the Kafka broker.

```elixir
iex> KafkaEx.create_worker(:pr) # where :pr is the process name of the created worker
{:ok, #PID<0.171.0>}
```

With custom options:

```elixir
iex> uris = [{"localhost", 9092}, {"localhost", 9093}, {"localhost", 9094}]
[{"localhost", 9092}, {"localhost", 9093}, {"localhost", 9094}]
iex> KafkaEx.create_worker(:pr, [uris: uris, consumer_group: "kafka_ex", consumer_group_update_interval: 100])
{:ok, #PID<0.172.0>}
```

### Create an unnamed KafkaEx worker

You may find you want to create many workers, say in conjunction with
a `poolboy` pool. In this scenario you usually won't want to name these worker processes.

To create an unnamed worked with `create_worker`:
```elixir
iex> KafkaEx.create_worker(:no_name) # indicates to the server process not to name the process
{:ok, #PID<0.171.0>}
```

### Use KafkaEx with a pooling library

Note that KafkaEx has a supervisor to manage its workers. If you are using Poolboy or a similar
library, you will want to manually create a worker so that it is not supervised by `KafkaEx.Supervisor`.
To do this, you will need to call:

```elixir
GenServer.start_link(KafkaEx.Config.server_impl,
  [
    [uris: KafkaEx.Config.brokers(),
     consumer_group: Application.get_env(:kafka_ex, :consumer_group)],
    :no_name
  ]
)
```

Alternatively, you can call

```
KafkaEx.start_link_worker(:no_name)
```

### Retrieve kafka metadata
For all metadata

```elixir
iex> KafkaEx.metadata
%KafkaEx.Protocol.Metadata.Response{brokers: [%KafkaEx.Protocol.Metadata.Broker{host:
 "192.168.59.103",
   node_id: 49162, port: 49162, socket: nil}],
 topic_metadatas: [%KafkaEx.Protocol.Metadata.TopicMetadata{error_code: :no_error,
   partition_metadatas: [%KafkaEx.Protocol.Metadata.PartitionMetadata{error_code: :no_error,
     isrs: [49162], leader: 49162, partition_id: 0, replicas: [49162]}],
   topic: "LRCYFQDVWUFEIUCCTFGP"},
  %KafkaEx.Protocol.Metadata.TopicMetadata{error_code: :no_error,
   partition_metadatas: [%KafkaEx.Protocol.Metadata.PartitionMetadata{error_code: :no_error,
     isrs: [49162], leader: 49162, partition_id: 0, replicas: [49162]}],
   topic: "JSIMKCLQYTWXMSIGESYL"},
  %KafkaEx.Protocol.Metadata.TopicMetadata{error_code: :no_error,
   partition_metadatas: [%KafkaEx.Protocol.Metadata.PartitionMetadata{error_code: :no_error,
     isrs: [49162], leader: 49162, partition_id: 0, replicas: [49162]}],
   topic: "SCFRRXXLDFPOWSPQQMSD"},
  %KafkaEx.Protocol.Metadata.TopicMetadata{error_code: :no_error,
...
```

For a specific topic

```elixir
iex> KafkaEx.metadata(topic: "foo")
%KafkaEx.Protocol.Metadata.Response{brokers: [%KafkaEx.Protocol.Metadata.Broker{host: "192.168.59.103",
   node_id: 49162, port: 49162, socket: nil}],
 topic_metadatas: [%KafkaEx.Protocol.Metadata.TopicMetadata{error_code: :no_error,
   partition_metadatas: [%KafkaEx.Protocol.Metadata.PartitionMetadata{error_code: :no_error,
     isrs: [49162], leader: 49162, partition_id: 0, replicas: [49162]}],
   topic: "foo"}]}
```

### Retrieve offset from a particular time

Kafka will get the starting offset of the log segment that is created no later than the given timestamp. Due to this, and since the offset request is served only at segment granularity, the offset fetch request returns less accurate results for larger segment sizes.

```elixir
iex> KafkaEx.offset("foo", 0, {{2015, 3, 29}, {23, 56, 40}}) # Note that the time specified should match/be ahead of time on the server that kafka runs
[%KafkaEx.Protocol.Offset.Response{partition_offsets: [%{error_code: :no_error, offset: [256], partition: 0}], topic: "foo"}]
```

### Retrieve the latest offset

```elixir
iex> KafkaEx.latest_offset("foo", 0) # where 0 is the partition
[%KafkaEx.Protocol.Offset.Response{partition_offsets: [%{error_code: :no_error, offset: [16], partition: 0}], topic: "foo"}]
```

### Retrieve the earliest offset

```elixir
iex> KafkaEx.earliest_offset("foo", 0) # where 0 is the partition
[%KafkaEx.Protocol.Offset.Response{partition_offsets: [%{error_code: :no_error, offset: [0], partition: 0}], topic: "foo"}]
```

### Fetch kafka logs

**NOTE** You must pass `auto_commit: false` in the options for `fetch/3` when using `:no_consumer_group`.

```elixir
iex> KafkaEx.fetch("foo", 0, offset: 5) # where 0 is the partition and 5 is the offset we want to start fetching from
[%KafkaEx.Protocol.Fetch.Response{partitions: [%{error_code: :no_error,
     hw_mark_offset: 115,
     message_set: [
      %KafkaEx.Protocol.Fetch.Message{attributes: 0, crc: 4264455069, key: nil, offset: 5, value: "hey"},
      %KafkaEx.Protocol.Fetch.Message{attributes: 0, crc: 4264455069, key: nil, offset: 6, value: "hey"},
      %KafkaEx.Protocol.Fetch.Message{attributes: 0, crc: 4264455069, key: nil, offset: 7, value: "hey"},
      %KafkaEx.Protocol.Fetch.Message{attributes: 0, crc: 4264455069, key: nil, offset: 8, value: "hey"},
      %KafkaEx.Protocol.Fetch.Message{attributes: 0, crc: 4264455069, key: nil, offset: 9, value: "hey"}
...], partition: 0}], topic: "foo"}]
```

### Produce kafka logs

```elixir
iex> KafkaEx.produce("foo", 0, "hey") # where "foo" is the topic and "hey" is the message
:ok
```

### Stream kafka logs

See the `KafkaEx.stream/3` documentation for details on streaming.

```elixir
iex> KafkaEx.produce("foo", 0, "hey")
:ok
iex> KafkaEx.produce("foo", 0, "hi")
:ok
iex> KafkaEx.stream("foo", 0, offset: 0) |> Enum.take(2)
[%{attributes: 0, crc: 4264455069, key: nil, offset: 0, value: "hey"},
 %{attributes: 0, crc: 4251893211, key: nil, offset: 1, value: "hi"}]
```

When using `:no_consumer_group`, you should pass `auto_commit: false`:

```elixir
iex> KafkaEx.stream("foo", 0, offset: 0, auto_commit: false) |> Enum.take(2)
```

### Compression

Snappy and gzip compression is supported.  Example usage for producing compressed messages:

```elixir
message1 = %KafkaEx.Protocol.Produce.Message{value: "value 1"}
message2 = %KafkaEx.Protocol.Produce.Message{key: "key 2", value: "value 2"}
messages = [message1, message2]

#snappy
produce_request = %KafkaEx.Protocol.Produce.Request{
  topic: "test_topic",
  partition: 0,
  required_acks: 1,
  compression: :snappy,
  messages: messages}
KafkaEx.produce(produce_request)

#gzip
produce_request = %KafkaEx.Protocol.Produce.Request{
  topic: "test_topic",
  partition: 0,
  required_acks: 1,
  compression: :gzip,
  messages: messages}
KafkaEx.produce(produce_request)
```

Compression is handled automatically on the consuming/fetching end.

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

To launch the included test cluster, run

```
./scripts/docker_up.sh
```

The `docker_up.sh` script will attempt to determine an IP address for your
computer on an active network interface.

The test cluster runs Kafka 0.11.0.1.

### Running the KafkaEx Tests

The KafkaEx tests are split up using tags to handle testing multiple scenarios
and Kafka versions.

#### Unit tests

These tests do not require a Kafka cluster to be running (see test/test_helper.exs:3 for the tags excluded when running this).

```
mix test --no-start
```

#### Integration tests

If you are not using the Docker test cluster, you may need to modify
`config/config.exs` for your set up.

The full test suite requires Kafka 2.1.0+.

Run the full integration test suite:

```
./scripts/all_tests.sh
```

Or run specific test categories:

```
mix test --include integration --include consumer_group
```

### Static analysis

```
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

## Default snappy algorithm use snappyer package

It can be changed to snappy by using this:

``` elixir
config :kafka_ex, snappy_module: :snappy
```

Snappy erlang nif is deprecated and will be dropped 1.0.0 release.
