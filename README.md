KafkaEx
========

![Build Status](https://travis-ci.org/kafkaex/kafka_ex.svg?branch=master)

[Apache Kafka](http://kafka.apache.org/) (>= 0.8.0) client for Elixir/Erlang.

Usage
-----

Add KafkaEx to your mix.exs dependencies:

```elixir
defp deps do
  [{:kafka_ex, "~> 0.3.0"}]
end
```

Add KafkaEx to your mix.exs applications:

```elixir
def application do
  [applications: [:kafka_ex]]
end
```

And run:

```
mix deps.get
```

*Note* If you wish to use snappy for compression or decompression, you
 must add
 [snappy-erlang-nif](https://github.com/fdmanana/snappy-erlang-nif) to
 your project's mix.exs. Also add snappy your application list, e.g:

```elixir
def application do
  [applications: [:kafka_ex, :snappy]]
end
```

 and to your deps list, e.g:

```elixir
defp deps do
  [applications: [
   {:kafka_ex, "0.3.0"},
   {:snappy, git: "https://github.com/fdmanana/snappy-erlang-nif"}
  ]]
end
```

### Configuration

In your config/config.exs add the list of kafka brokers as below:
```elixir
config :kafka_ex,
  brokers: [{HOST, PORT}],
  consumer_group: consumer_group #if no consumer_group is specified "kafka_ex" would be used as the default
```

Alternatively from iex:
```elixir
iex> Application.put_env(:kafka_ex, :brokers, [uris: [{"localhost", 9092}, {"localhost", 9093}], consumer_group: "kafka_ex"])
:ok
```

You can also override options when creating a worker, see below.

### Create KafkaEx worker
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

### Create KafkaEx worker

You may find you want to create many workers, say in conjunction with
a `poolboy` pool. In this scenario you usually won't want to name these worker processes.

To create an unnamed worked with `create_worker`:
```elixir
iex> KafkaEx.create_worker(:no_name) # indicates to the server process not to name the process
{:ok, #PID<0.171.0>}
```

To create an unnamed worker directly with the `KafkaEx.Supervisor`:
```elixir
iex> worker_init = [] # whatever options you want
iex> Supervisor.start_child(KafkaEx.Supervisor, [worker_init, :no_name]) # indicates to the server process not to name the process
{:ok, #PID<0.171.0>}
```

### Retrieve kafka metadata
For all metadata

```elixir
iex> KafkaEx.metadata
%KafkaEx.Protocol.Metadata.Response{brokers: [%KafkaEx.Protocol.Metadata.Broker{host:
 "192.168.59.103",
   node_id: 49162, port: 49162, socket: nil}],
 topic_metadatas: [%KafkaEx.Protocol.Metadata.TopicMetadata{error_code: 0,
   partition_metadatas: [%KafkaEx.Protocol.Metadata.PartitionMetadata{error_code: 0,
     isrs: [49162], leader: 49162, partition_id: 0, replicas: [49162]}],
   topic: "LRCYFQDVWUFEIUCCTFGP"},
  %KafkaEx.Protocol.Metadata.TopicMetadata{error_code: 0,
   partition_metadatas: [%KafkaEx.Protocol.Metadata.PartitionMetadata{error_code: 0,
     isrs: [49162], leader: 49162, partition_id: 0, replicas: [49162]}],
   topic: "JSIMKCLQYTWXMSIGESYL"},
  %KafkaEx.Protocol.Metadata.TopicMetadata{error_code: 0,
   partition_metadatas: [%KafkaEx.Protocol.Metadata.PartitionMetadata{error_code: 0,
     isrs: [49162], leader: 49162, partition_id: 0, replicas: [49162]}],
   topic: "SCFRRXXLDFPOWSPQQMSD"},
  %KafkaEx.Protocol.Metadata.TopicMetadata{error_code: 0,
...
```

For a specific topic

```elixir
iex> KafkaEx.metadata(topic: "foo")
%KafkaEx.Protocol.Metadata.Response{brokers: [%KafkaEx.Protocol.Metadata.Broker{host: "192.168.59.103",
   node_id: 49162, port: 49162, socket: nil}],
 topic_metadatas: [%KafkaEx.Protocol.Metadata.TopicMetadata{error_code: 0,
   partition_metadatas: [%KafkaEx.Protocol.Metadata.PartitionMetadata{error_code: 0,
     isrs: [49162], leader: 49162, partition_id: 0, replicas: [49162]}],
   topic: "foo"}]}
```

### Retrieve offset from a particular time

Kafka will get the starting offset of the log segment that is created no later than the given timestamp. Due to this, and since the offset request is served only at segment granularity, the offset fetch request returns less accurate results for larger segment sizes.

```elixir
iex> KafkaEx.offset("foo", 0, {{2015, 3, 29}, {23, 56, 40}}) # Note that the time specified should match/be ahead of time on the server that kafka runs
[%KafkaEx.Protocol.Offset.Response{partition_offsets: [%{error_code: 0, offset: [256], partition: 0}], topic: "foo"}]
```

### Retrieve the latest offset

```elixir
iex> KafkaEx.latest_offset("foo", 0) # where 0 is the partition
[%KafkaEx.Protocol.Offset.Response{partition_offsets: [%{error_code: 0, offsets: [16], partition: 0}], topic: "foo"}]
```

### Retrieve the earliest offset

```elixir
iex> KafkaEx.earliest_offset("foo", 0) # where 0 is the partition
[%KafkaEx.Protocol.Offset.Response{partition_offsets: [%{error_code: 0, offset: [0], partition: 0}], topic: "foo"}]
```

### Fetch kafka logs

```elixir
iex> KafkaEx.fetch("foo", 0, offset: 5) # where 0 is the partition and 5 is the offset we want to start fetching from
[%KafkaEx.Protocol.Fetch.Response{partitions: [%{error_code: 0,
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

```elixir
iex> KafkaEx.create_worker(:stream, [uris: [{"localhost", 9092}]])
{:ok, #PID<0.196.0>}
iex> KafkaEx.produce("foo", 0, "hey", :stream)
:ok
iex> KafkaEx.produce("foo", 0, "hi", :stream)
:ok
iex> KafkaEx.stream("foo", 0) |> Enum.take(2)
[%{attributes: 0, crc: 4264455069, key: nil, offset: 0, value: "hey"},
 %{attributes: 0, crc: 4251893211, key: nil, offset: 1, value: "hi"}]
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
  required_acks: 1,
  compression: :snappy,
  messages: messages}
KafkaEx.produce(produce_request)

#gzip
produce_request = %KafkaEx.Protocol.Produce.Request{
  topic: "test_topic",
  required_acks: 1,
  compression: :gzip,
  messages: messages}
KafkaEx.produce(produce_request)
```

Compression is handled automatically on the consuming/fetching end.

### Test

#### Unit tests
```
mix test --no-start
```

#### Integration tests
Add the broker config to `config/config.exs` and run:
##### Kafka >= 0.8.2
```
mix test --only consumer_group --only integration
```
##### Kafka < 0.8.2
```
mix test --only integration
```

#### All tests
##### Kafka >= 0.8.2
```
mix test --include consumer_group --include integration
```
##### Kafka < 0.8.2
```
mix test --include integration
```

### Static analysis

```
mix dialyze --unmatched-returns --error-handling --race-conditions --underspecs
```

### Contributing
Please see [CONTRIBUTING.md](CONTRIBUTING.md)
