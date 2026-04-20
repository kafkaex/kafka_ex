# KafkaEx Changelog

## 1.0.0

Single release entry accumulates all post-rc.2 work. RC tags along the
way (rc.3, rc.4, …) share this entry — individual rc entries are not
maintained.

### Breaking Changes

* **Produce headers API** — the `headers:` option on
  `KafkaEx.API.produce/4,5`, `produce_one/4,5`, and `produce_sync/4,5`
  now requires `[%KafkaEx.Messages.Header{}]` structs instead of
  `[{binary, binary}]` tuples. Brings the produce path in line with
  the fetch path, which already returned `%Header{}` structs.

  Before:
  ```elixir
  KafkaEx.API.produce(client, "t", 0, [
    %{value: "v", headers: [{"key", "val"}]}
  ])
  ```

  After:
  ```elixir
  alias KafkaEx.Messages.Header
  KafkaEx.API.produce(client, "t", 0, [
    %{value: "v", headers: [Header.new("key", "val")]}
  ])
  ```

  Silent-at-compile, fails-at-runtime with `FunctionClauseError` —
  see UPGRADING.md for the migration pattern.

### Fixed

* **Consumer group no longer silently consumes on stale generation**
  after a non-retryable `OffsetCommit` error. Previously
  `:illegal_generation` (and siblings) were logged and swallowed;
  the consumer kept running on a stale generation until the next
  heartbeat happened to also fail, potentially many seconds of
  zombie operation. Now classified across three paths matching
  Java `ConsumerCoordinator`, librdkafka `rdkafka_cgrp`, brod
  `brod_group_coordinator`, and kafka-python:

  - **Terminal** — `:fenced_instance_id` (KIP-345),
    `:group_authorization_failed`, `:topic_authorization_failed`,
    `:offset_metadata_too_large`, `:invalid_commit_offset_size`.
    Consumer stops without rejoining.
  - **Fatal** — `:illegal_generation`, `:unknown_member_id`.
    GenConsumer casts `{:rejoin_required, reason, stale_gen}` to
    the group manager (with mailbox coalescing for multi-partition
    storms), self-stops with `{:shutdown, {:rejoin_required, _}}`.
    Manager resets member_id / generation_id and rebalances. Under
    `restart: :transient` the supervisor does not respawn the
    stopped worker; the rebalance spawns a fresh one.
  - **Retryable** — `:rebalance_in_progress` (flag-and-wait;
    heartbeat path drives the eventual rebalance, matching Java),
    `:unstable_offset_commit` (KIP-447), and standard transient
    errors.

  At-least-once semantics preserved. See
  [UPGRADING.md](./UPGRADING.md) for details.

* **Bootstrap crash** when `api_version` was set explicitly and the
  broker map was empty — the client now honors the explicit
  override instead of returning `:api_not_supported_by_broker`.

* **Record-header encoding** in V3+ record batches (per-record
  headers were previously dropped).

* **Version-0 falsy bug** in `get_api_version` — V0 was treated as
  "unset" and fell through to the hardcoded default.

### Added

* **KIP-394 two-step JoinGroup.** Client auto-retries `JoinGroup`
  with the broker-assigned `member_id` on `:member_id_required`.
  Required for interop with Kafka 2.3+ under
  `group.initial.rebalance.delay.ms`.

* **KIP-345 batch LeaveGroup V3+.** `LeaveGroup` sends the `members`
  array for V3+ brokers.

* **3-tier API version resolution.**
  1. Per-request `:api_version` option
  2. Application config `api_versions: %{...}` map
  3. Broker-negotiated max (`min(broker_max, kayrock_max)`)

  Replaces hardcoded defaults that ignored broker capability.
  Previously produce/fetch used v3 regardless of what the broker
  supported; now a Kafka 3.x broker will get the full API surface.

* **`KafkaEx.Support.VersionHelper.maybe_put_api_version/3`** —
  internal helper for consumer/stream callers. Thin wrapper over
  the 3-tier resolution.

* **`[:kafka_ex, :consumer, :commit_failed]` telemetry event.**
  Emitted on every commit-error branch (terminal / fatal /
  transient) with metadata
  `%{group_id, topic, partition, offset, kind, error}`. Parity
  stand-in for Java's `CommitFailedException` — subscribe to the
  event to observe commit failures.

* **Retry classifiers.**
  `KafkaEx.Support.Retry.commit_fatal_error?/1` and
  `commit_terminal_error?/1` — mirrors the reference-client error
  taxonomy.

### Changed

* **Test infrastructure.** `Process.sleep(50)` replaced with
  `MockClient.wait_for_calls/3` polling helper for deterministic
  unit tests. Adds lifecycle integration tests for the 3-tier
  version resolution and the `:illegal_generation` rejoin loop.

* **Integration + chaos coverage for Phase B.** Live-broker
  integration tests for rejoin_required cast handling, plus a
  Testcontainers-isolated chaos test that fires a 30-second storm
  of fatal casts and asserts the mailbox-drain coalescing ratio.

### Migration

See [UPGRADING.md](./UPGRADING.md).

## 1.0.0-rc.2 - 2026-03-15

### Fixed

* `ConsumerGroupDescription.Member.member_assignment` now returns an empty `%MemberAssignment{version: 0, user_data: <<>>, partition_assignments: []}` struct instead of `nil` when Kafka returns a null or empty member assignment (e.g., during rebalancing). This prevents `KeyError` crashes when accessing `.partition_assignments` on the result. **Breaking:** code that pattern-matches on `member_assignment: nil` or checks `== nil` must be updated to check for an empty `partition_assignments` list instead.

### Changed

* `MemberAssignment` defstruct now has explicit defaults (`version: 0`, `partition_assignments: []`, `user_data: <<>>`) matching its typespec.
* Type specs for `Member.t()` and `Member.assignment/1` no longer include `| nil` for the `member_assignment` field.

## 1.0.0-rc.1 - 2026-02-15

### Breaking Changes

* Removed legacy server implementations (Server0P8P0, Server0P8P2, Server0P9P0, Server0P10AndLater)
* Removed `kafka_version` configuration option - Kayrock is now the only implementation
* Kayrock is now the default and only client implementation
* Module reorganization:
  * `KafkaEx.GenConsumer` → `KafkaEx.Consumer.GenConsumer`
  * `KafkaEx.ConsumerGroup` → `KafkaEx.Consumer.ConsumerGroup`
  * `KafkaEx.New.Kafka.*` → `KafkaEx.Messages.*`
  * `KafkaEx.New.Client` → `KafkaEx.Client`
  * `KafkaEx.New.KafkaExAPI` → `KafkaEx.API`

### Added

* `KafkaEx.API` module as primary API with explicit client-based functions
* Automatic API version negotiation with Kafka brokers
* Full message headers support
* SASL authentication support (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, OAUTHBEARER, MSK_IAM)
* Telemetry support for request lifecycle monitoring and observability
* Heartbeat API (v0-v4) with full version support
* LeaveGroup API (v0-v4) with full version support
* SyncGroup API (v0-v4) with full version support
* Graceful shutdown for GenConsumer
* Compression support (gzip, snappy, lz4, zstd)

### Changed

* All functions now use Kayrock for protocol handling
* Improved error handling with structured errors (`KafkaEx.Client.Error`)
* Codebase reorganized by domain (cluster, client, consumer, producer, messages)
* Test structure reorganized to match new module organization

### Removed

* Legacy `KafkaEx.produce/4` and `KafkaEx.fetch/3` - use `KafkaEx.API` functions
* `KafkaEx.Server0P8P0`, `KafkaEx.Server0P8P2`, `KafkaEx.Server0P9P0`, `KafkaEx.Server0P10AndLater`
* `kafka_version` configuration option
* Legacy snappy-erlang-nif dependency (snappyer is now the default)

### Migration

See UPGRADING.md for detailed migration instructions.

---

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
