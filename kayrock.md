# Kayrock-Based KafkaEx Client

This document contains information on using the new
[Kayrock](https://github.com/dantswain/kayrock)-based KafkaEx client.

The client itself is defined in the module `KafkaEx.New.Client`.  This will be
the primary KafkaEx client module going forward.

**NOTE** In many places below we recommend using "version N and up".  The
reality is that in many cases, the KafkaEx legacy API compatibility only
supports versions 0 to 3.  Using version 3 should generally be safe and achieve
the desired outcomes.  The new API will be designed to handle newer versions.

Contents:

*   [Using the New Client](#using-the-new-client)
*   [Common Use Case - Store Offsets In
     Kafka](#common-use-case-store-offsets-in-kafka)
*   [Common Use Case - Message Timestamps / New Storage Format](#common-use-case-message-timestamps-new-storage-format)

## Using the New Client

To use the new client in your project, set `kafka_version: "kayrock"` in your
config file:

```
config :kafka_ex,
  kafka_version: "kayrock"
```

If want to start a single client instance, supply the new client's module
name to the `:server_impl` option of `KafkaEx.start_link_worker/2`:

```
{:ok, pid} = KafkaEx.start_link_worker(:no_name, server_impl: KafkaEx.New.Client)
```

The new client should be totally backwards compatible with the legacy API;
simply use KafkaEx like you normally would.  It is also compatible with the new
API defined in `KafkaEx.New.KafkaExAPI`.

## Common Use Case - Store Offsets In Kafka

Offsets are stored in Kafka (instead of Zookeeper) with offset commit message
version 1 and up (Kafka v0.10 and up). Message version 1 also includes a
timestamp parameter that was dropped in version 2, so we recommend using at
least version 2. We often use version 3 out of convenience because that
version number achieves desired results with this and other Kafka API
messages.

To retrieve offsets committed with version 1 and up, you must also use version 1
and up of the offset fetch request.  A safe move here is to always use the same
version for offset commit and offset fetch.

### WARNING - OFFSET LOSS

If offsets for a consumer group are stored in Zookeeper (v0 KafkaEx legacy),
they are unavailable using v1 and up of the offset fetch message.  This means
that if you have an existing KafkaEx consumer group and "upgrade" to the new
version, **you will lose offsets**.  To avoid losing offsets, you should first
convert the Zookeeper offsets to Kafka storage.  This can be achieved using
command line tools (documentation of which is beyond the scope of this document)
or using KafkaEx by fetching the offsets using v0 and then committing them using
v1.  We may provide a tool for this in the future.

Likewise, once you store offsets in Kafka, they cannot be fetched using v0 of
the offset fetch message.  If you need to "roll back" storage from Kafka to
Zookeeper, you will need to convert the offsets first.

### Examples

`KafkaEx.offset_commit/2` and `KafkaEx.offset_fetch/2` support setting the api
version via the `api_version` field of their corresponding requests structs:

```
alias KafkaEx.Protocol.OffsetCommit

# commit offsets using kafka storage
KafkaEx.offset_commit(client, %OffsetCommit.Request{
        consumer_group: consumer_group,
        topic: topic,
        partition: 0,
        offset: offset,
        api_version: 3
      })

# fetch an offset stored in kafka
[resp] = KafkaEx.offset_fetch(client, %OffsetFetch.Request{
        topic: topic,
        consumer_group: consumer_group,
        partition: 0,
        api_version: 3
      })
%KafkaEx.Protocol.OffsetFetch.Response{partitions: [%{offset: offset}]} = resp
```

When using `KafkaEx.fetch/3` with `auto_commit: true`, you can specify the
`offset_commit_api_version` option to control how offsets are stored:

```
# store auto-committed offsets in kafka
KafkaEx.fetch(topic, partition, auto_commit: true, offset_commit_api_version: 3)
```

When using `KafkaEx.ConsumerGroup`, you can control offset storage using the
`api_versions` option:

```
# use kafka offset storage with consumer groups
# NOTE you must use compatible version for offset_fetch and offset_commit 
#   using the same value for both should be safe
KafkaEx.ConsumerGroup.start_link(
    MyConsumer,
    consumer_group_name,
    [topic],
    api_versions: %{offset_fetch: 3, offset_commit: 3}
)
```

## Common Use Case - Message Timestamps / New Storage Format

Message timestamps and the [new message storage
format](https://kafka.apache.org/documentation/#recordbatch) go hand-in-hand
because they both require setting the message versions for produce and fetch.

Timestamps were added in v1 of the produce/fetch messages, but the storage
format was replaced in v2 (around Kafka v0.10), so we recommend using 2 and up
(3 is safe).

### WARNING - Broker Performance

Check with your system administrator.  If the broker is configured to use the
new message format (v2 and up), producing or requesting messages with old
formats (v0 and v1) can lead to significant load on the brokers because they
will need to convert messages between versions for each request.  If you have a
relatively modern version of Kafka, we recommend using version 3 for both
messages.

### Examples

Whenever we use produce API version `>= 2`, the new message format is used automatically.

To publish a message with a timestamp:

```
{:ok, offset} =
  KafkaEx.produce(
    topic,
    0,
    msg,
    worker_name: client,
    required_acks: 1,
    timestamp: 12345,
    api_version: 3
  )

fetch_responses =
  KafkaEx.fetch(topic, 0,
    offset: offset,
    auto_commit: false,
    worker_name: client,
    api_version: 3
  )

[fetch_response | _] = fetch_responses
[partition_response | _] = fetch_response.partitions
message = List.last(partition_response.message_set)

message.value # => msg
message.offset # => offste
message.timestamp # => 12345
```

If a topic has the `message.timestamp.type` setting set to `LogAppendTime`, then
timestamps will be populated automatically when a produced message is received
by the broker and appended to the log.

```
fetch_responses =
  KafkaEx.fetch(topic_with_log_append_timestamps, 0,
    offset: offset,
    auto_commit: false,
    worker_name: client,
    api_version: 3
  )

[fetch_response | _] = fetch_responses
[partition_response | _] = fetch_response.partitions
message = List.last(partition_response.message_set)

message.timestamp # => log append timestamp in milliseconds
```

Note that the `KafkaEx.ConsumerGroup` `api_versions` option also supports
setting a version for `fetch`:

```
# use new record batch format AND kafka offset storage with consumer groups
KafkaEx.ConsumerGroup.start_link(
    MyConsumer,
    consumer_group_name,
    [topic],
    api_versions: %{offset_fetch: 3, offset_commit: 3, fetch: 3}
)
```
