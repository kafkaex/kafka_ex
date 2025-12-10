# KafkaEx Changelog

## Unreleased

## Features

* Added `SASL` Auth Implementation
* Added Graceful shutdown for GenConsumer
* Migrated Offset API to `kayrock`
* Migrated Consumer Group API to `kayrock`
* Migrated LeaveGroup API (v0-v1) to `kayrock` with new API support
* Migrated SyncGroup API (v0-v1) to `kayrock` with new API support

## 0.14

### Fixes

* Multiple Github Action Fixes
* Fix deprecation warnings with Bitwise usage
* Fix deprecation warnings with Config
* Fix deprecation warnings with Stacktrace

### Features

* Added `describe_groups` API

### Breaking Changes

* Set minimal version of elixir to 1.8

## 0.13

*   Support Snappyer 2
*   Continuous integration: replace CircleCI
*   Using the Kayrock client: take into account the `api_version` when retrieving offsets to fetch messages.
*   Update metadata before topic creation to make sure it connects to a  controller broker.
*   Support record headers.

## 0.12.1

Increases the version of ex_doc to allow publishing

Includes all the 0.12.0 changes.

## 0.12.0

NOTE: not released due to issues with ex_doc.

### Breaking Changes

*   Drop support for Elixir 1.5

### Features

*   Allow passthrough of ssl_options when starting a consumer or consumer group (#413)
*   Allow GenConsumer callbacks to return `:stop` - allows graceful shutdown of consumers (#424)

### Bugfixes

### Misc

*   Use Dynamic Supervisor rather than simple_one_for_one - fixes warnings on newer elixir versions (#418)
*   Update to Kayrock 0.1.12 - fix compile warnings (#419)
*   Tests pass the first time more often now (#420)
*   Fix deprecation warning about `Supervisor.terminate_child` (#430)
*   Remove Coveralls - was not being used, caused test failures (#423)

### PRs Included

*   #413
*   #418
*   #419
*   #420
*   #430
*   #423
*   #424

## 0.11.0

KafkaEx 0.11.0 is a large improvement to KafkaEx that sees the introduction of the Kayrock client, numerous stability fixes, and a critical fix that eliminates double-consuming messages when experiencing network failures under load.

### Breaking changes

*   Drop support for Elixir < 1.5
*   Partitioner has been fixed to match the behavior of the Java client. This will cause key assignment to differ. To keep the old behavior, use the KafkaEx.LegacyPartitioner module. (#399)

### Fixes

*   Numerous fixes to error handling especially for networking connections (#347, #351 )
*   Metadata is refreshed after topic create/delete(#349)
*   Compression actually works for producing now 😬  (#362)
*   Don't crash when throttled by quotas (#402)
*   Default partitioner works the same way as the Java client now (#399)
*   When fetching metadata for a subset of topics, don't remove the metadata for the unfetched topics. (#409)

### Improvements

*   Any GenServer can be used as a KafkaEx.GenConsumer (#339)
*   Can specify wait time before attempting reconnect (#347)
*   KafkaEx.stream/3 now uses the KafkaEx.Server interface, which inherits the sync_timeout setting. This avoids timeouts when sync_timeout needs to be greater than default. (#354)
*   KafkaEx can now use [Kayrock](https://github.com/dantswain/kayrock) as the client implementation. This is a large change, and is pushing toward the improvements we want in 1.0 to allow the library to easily support new versions of the Kafka protocol. (#356, #359, #364, #366, #367, #369, #370, #374, #375, #377, #379, #387,  #406, #408) 
*   `KafkaEx.Protocol.Fetch.Message` includes the topic and partition, allowing consumers to know which topic and partition they consumed from.
*   Add `KafkaEx.start_link_worker/1-2` to start a working and link it to the current process.
*   Allow setting the client_id for the application - supports better monitoring, debugging, and quotas. (#388)
*   Send metadata requests to a random broker rather than the same one each time (#395)
*   Retry joining a consumer group 6 times rather than failing (#375, #403)

#### Kayrock client

See [Kayrock and the Future of KafkaEx](https://github.com/kafkaex/kafka_ex#important---kayrock-and-the-future-of-kafkaex)

Note that the Kayrock implementation doesn't support Kafka < 0.11

Improvements over default client:
*   Can specify message API versions for the `KafkaEx.stream/3` API
*   Can specify message API versions for the consumer group API - NOTE that if you specify OffsetCommit API version >= 2, it will attempt to store the offsets in kafka, which will have no data about the topic unless you migrate the data. This could result in losing or reprocessing data. Migration can be done out of band, or can be done via the appropriate API calls within KafkaEx.
*   Allow specifying OffsetCommit API version in KafkaEx.fetch

### Misc

*   Test suite uses KafkaEx 0.11 now in preparation for fully supporting Kafka API versions.
*   KafkaEx.Protocol.Produce cleaned up (#380) 
*   Documentation improvements (#383, #384)
*   Test against Elixir 1.9 (#394)
*   Use OTP 22.3.3+ for OTP 22.2 testing to avoid SSL bug. (#405)

## 0.10.0

### Features
*   Allow passing in state to a `KafkaEx.GenConsumer` by defining a `KafkaEx.GenConsumer.init/3` callback. Adds a default implementation of `init/3` to ensure backward compatibility. -- @mtrudel 
*   Support DeleteTopics API for Kafka 0.10+ -- @jbruggem 
*   Add a default partitioner using murmur2 hashing when key is provided, or random partitioning otherwise. Use the `KafkaEx.Partitioner` behaviour to define a different partitioner, and enable it by adding it to the `partitioner` configuration setting.

### Misc
*   Lots of documentation fixes
*   Fixing elixir 1.8 compile warnings

### PRs Included:

*   #343
*   #338
*   #337
*   #335
*   #329
*   #333
*   #331

## Previous Versions

See the releases for change notes.
