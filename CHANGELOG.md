# KafkaEx Changelog

## Unreleased

### Added

* **Static consumer-group membership (KIP-345).** New `:group_instance_id` option on
  `KafkaEx.Consumer.ConsumerGroup` (resolved `opts > app-env > nil`, default off). When set, the
  member presents a stable instance id on JoinGroup/SyncGroup/Heartbeat and **does not send
  LeaveGroup on shutdown**, so the broker retains its partition assignment across a restart
  (until `session.timeout.ms`) instead of rebalancing. Requires Kafka >= 2.3; a too-old broker
  logs a warning and the consumer runs dynamically. A duplicate id is fenced
  (`:fenced_instance_id` → terminal stop, `member_terminated{terminal_class: :fenced}`).

* **Per-attempt request timeout is now configurable via `:request_timeout`** (default
  `15_000` ms) — the `Socket.recv` deadline for a single synchronous broker request attempt
  (metadata, offset commit/fetch, heartbeat, produce ack, …). Consumer-group JoinGroup/SyncGroup
  derive their own, longer per-attempt deadlines from the group's rebalance/session timeouts.

* **Retry backoff now applies ±20% jitter (KIP-580)** in `KafkaEx.Support.Retry.with_retry/2`,
  decorrelating retries across many consumer-group members after a shared coordinator/broker blip.

### Fixed

* **Consumer-group cold start no longer times out (JoinGroup/SyncGroup socket timeouts).**
  JoinGroup/SyncGroup previously fell back to the `1000` ms generic socket-recv timeout, while the
  broker legitimately holds a JoinGroup response for up to `group.initial.rebalance.delay.ms`
  (default 3 s) on a cold/empty group — so the first member reliably timed out and never joined.
  (The library's own `config.exs` value never propagated to consumers, so the effective default
  was the `1000` ms code fallback.) JoinGroup's per-attempt deadline is now `rebalance_timeout +
  5000` and SyncGroup's is derived from the session window; both are sent **send-once** at the
  client, with `ConsumerGroup.Manager` owning rejoin — matching the Java/kafka-python/librdkafka
  model. The outer `GenServer.call` budget is derived from the per-attempt deadline so the two can
  no longer mismatch (the #357 class of bug). A socket recv-timeout is no longer retried inside the
  synchronous client loop; protocol errors keep their retry + metadata/coordinator refresh.

* **The abnormal heartbeat-crash rejoin loop is now bounded (#560).** When a
  consumer-group member's heartbeat process dies abnormally — an uncatchable
  `:kill`, OOM, or unstructured crash — the manager rejoins in place, but if it
  keeps dying more than `crash_rejoin_max_restarts` times within
  `crash_rejoin_window_ms` (defaults `10` / `60_000` ms; OTP
  `max_restarts`/`max_seconds` semantics) the member now stops terminally
  instead of looping forever. **Behavior change:** such a member previously
  looped indefinitely (silent, consuming nothing); it now stops, emits
  `[:kafka_ex, :consumer, :member_terminated]` with reason `{:crash_loop, _}`,
  and is torn down by its supervisor — delegating recovery to *your* supervisor.
  Set `crash_rejoin_max_restarts: :infinity` to restore the old unbounded
  behavior. A `ConsumerGroup` started **without** a supervising parent now stops
  on a crash-loop instead of looping — see UPGRADING.md. New observability: a
  `[:kafka_ex, :consumer, :heartbeat_crash]` event fires on each abnormal-crash
  rejoin (with `crashes_in_window`) for pre-terminal alerting, and
  `member_terminated` metadata gains a low-cardinality `terminal_class` atom
  (`:fenced | :auth | :crash_loop | :crashed | :error | :client_died | :other`)
  for alert routing.

### Deprecated

* **`:sync_timeout` is deprecated in favour of `:request_timeout`** and will be removed in
  KafkaEx 2.0. It remains honored as an alias; setting it without `:request_timeout` logs a
  one-time deprecation warning at client boot. See UPGRADING.md.

## 1.0.1 (2026-06-26)

### Breaking Changes

* **`KafkaEx.API.start_client/1` no longer registers globally by default.**
  Previously `start_client()` (no `:name` argument) silently registered
  the GenServer as `KafkaEx.Client`, while the documented `:name` option
  was ignored. Two callers collided with `:already_started`.

  Now `start_client()` returns `{:ok, pid}` unnamed. To preserve the old
  behavior explicitly:

  ```elixir
  KafkaEx.API.start_client(name: KafkaEx.Client)
  ```

  `:name` accepts any `GenServer.name()` shape: an atom,
  `{:global, term}`, or `{:via, Module, term}`. See UPGRADING.md for the
  full migration. Most callers bind the returned pid
  (`{:ok, client} = start_client(...)`) and are unaffected.

* **Consumer `auto_offset_reset` now defaults to `:latest`** (was `:none`),
  matching the Kafka/Java client default, and `:none` now *raises* instead of
  silently starting a fresh consumer group from the earliest offset. This only
  affects a consumer with no valid committed offset that does not set
  `auto_offset_reset` explicitly: a new consumer group now consumes only new
  messages (previously it silently replayed the whole topic), and an
  out-of-range offset now resets to latest (previously it raised). Set
  `auto_offset_reset: :earliest` to keep replaying from the beginning, or
  `:none` for the strict raise-on-bad-offset behavior. An invalid value now
  fails fast at consumer start. See UPGRADING.md.

### Fixed

* **`KafkaEx.API.start_client/1` and `child_spec/1` now merge `config.exs`
  defaults and accept the documented `:brokers` option (#538).** Previously
  `start_client(brokers: [...])` crashed with `InvalidConsumerGroupError: nil`
  because config defaults were never merged and the `:brokers` key was ignored
  (the client read `:uris`). `:brokers` is now the canonical key; `:uris` is a
  deprecated-but-working alias. `start_client/1` returns
  `{:error, :invalid_consumer_group}` on a bad group; `child_spec/1` raises
  `KafkaEx.InvalidConsumerGroupError` at spec-build time.

* **`KafkaEx.API.fetch/5` (and `KafkaEx.Consumer.Stream`) hang at logend.**
  Previously the GenServer.call default timeout (5s) and the per-broker
  `Socket.recv` timeout (1-3s from `sync_timeout` config) were not
  aligned with the user-supplied `:max_wait_time` (default 10s). The
  broker held the long-poll for up to 10s; both upper timeouts fired
  first, causing `socket close → reconnect → retry` 3 times, every
  retry hitting the same mismatch.

  Now `KafkaEx.API.fetch/5` derives both timeouts from `:max_wait_time`
  automatically (network_timeout = `max_wait_time + 5_000`,
  call_timeout = `network_timeout × 3 + 5_000`). The `× 3` multiplier
  covers the Client's retry budget so the GenServer.call does not exit
  while retries are still in flight. Users who bumped `sync_timeout` as
  a workaround can revert.

  Two related stream bugs also fixed:
  - `KafkaEx.Consumer.Stream` no longer silently spins when a fetch
    returns an error and `no_wait_at_logend: false`. The stream
    halts and emits a `Logger.warning` with the error code.
  - `auto_commit: true` + error response no longer raises `KeyError`
    on `fetch_response.message_set`. `need_commit?/2` returns false
    for error responses, so no commit is attempted.

* **KIP-394 two-step `JoinGroup` no longer retries on `:member_id_required`
  (#541).** The generic retry loop treated `:member_id_required` as a
  transient error and blindly re-sent the identical request, which the
  broker rejected again. `JoinGroup` now uses a dedicated
  `KafkaEx.Support.Retry.join_group_retryable?/1` classifier that returns
  `false` for `:member_id_required`, so the two-step path swaps in the
  broker-assigned `member_id` and rejoins as KIP-394 intends. Required for
  correct interop with Kafka 2.3+ under
  `group.initial.rebalance.delay.ms`.

* **Consumer-group heartbeat and SyncGroup errors now rejoin or stop
  cleanly instead of crashing the group (#554, #555).** Previously
  `:illegal_generation` / `:unknown_member_id` / `:fenced_instance_id`
  reached the manager as a generic error and crashed it under the
  `one_for_all` / `max_restarts: 0` consumer-group supervisor, escalating
  one member's stale generation into a group-wide rebalance. Now, matching
  brod's identity handling (terminal codes follow the Java client):

  - `:illegal_generation` → rejoin keeping `member_id` (only the
    generation is stale).
  - `:unknown_member_id` → rejoin after clearing `member_id` (the
    coordinator forgot the member; the broker assigns a fresh one).
  - `:fenced_instance_id` (KIP-345) and `:group_authorization_failed` →
    clean terminal stop without rejoining (rejoining would split-brain
    the static slot or fail authorization again).

  Stale heartbeat-timer EXITs (from a timer already replaced during a
  rebalance) are dropped instead of acted on. A `{:heartbeat_rejoin,
  reason}` reason is emitted on the `[:kafka_ex, :consumer, :rebalance]`
  telemetry event.

* **Client retry loop preserves the real transport error atom (#544).**
  A `recv` timeout to a broker was mapped to `:unknown`, which crashed the
  consumer-group manager via `SyncGroupError` (the rebalance-storm root
  cause). The actual transport atom (`:timeout`, `:closed`, …) is now kept
  so it is classified and recovered correctly.

* **Consumer-group sync recovery is bounded, not crash-on-error (#546).**
  Recoverable `SyncGroup` errors now rejoin with a bounded retry budget and
  generation reset between attempts instead of raising and tearing down the
  group supervisor.

* **The group coordinator is re-discovered on `:not_coordinator` /
  `:coordinator_not_available` (#547).** Previously a stale cached
  coordinator was reused after the coordinator moved (rolling restart),
  so every subsequent request failed. The cached coordinator is now
  invalidated and rediscovered.

* **OffsetCommit / OffsetFetch correctness — no silent loss or duplicate
  processing (#548).** The stream halts on a fatal commit error instead of
  looping and reprocessing; `load_offsets` retries retryable errors
  (including `:unstable_offset_commit`, KIP-447) and raises on
  fatal/exhaustion instead of silently resetting to the earliest offset.

* **Broker connect is bounded by a timeout instead of blocking
  indefinitely (#556).** Connecting to an unreachable broker no longer
  hangs the client; the connect is bounded by `:connect_timeout`
  (default 10s).

* **Transaction control batches are dropped from fetch results, and the
  fetch offset advances past them (#557).** Control batches (transaction
  commit/abort markers) were surfaced to consumers as records; they are now
  filtered out. A fetch that contains only control batches still advances
  the consumer/stream offset past them (via the batch metadata) instead of
  re-fetching the same offset forever.

### Internal

* Consumer-group / config test stabilization: eliminated the `on_exit`
  teardown `:noproc` race class via `KafkaEx.TestSupport.ProcessHelpers`
  and stabilized three flaky consumer-group / config tests (#549, #553).
* `load_offsets` startup retry now rides the unified
  `KafkaEx.Support.Retry.with_retry/2` with exponential backoff (#551).
* Renamed the `KafkaEx.Support.Retry` option `:max_retries` to
  `:max_attempts` for consistency with the retry-count semantics (#552).
* Migrated the test suite's mocking from Hammox to Mimic (`mimic ~> 1.7`),
  dropping the unused `KafkaEx.NetworkClientMock`. No runtime/production
  changes. (#534)
* Added regression test coverage pinning previously-untested behavior:
  remote-close (`:tcp_closed`/`:ssl_closed`) handling and its telemetry in
  `KafkaEx.Client` (#449), init fail-fast when all brokers are unreachable
  (#298), and cross-topic response identity on a single client (#445). (#535)

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

### Removed

* **`:consumer_group_update_interval` config option** (and
  corresponding `Client.State` field). The value was silent-ignored
  long before 1.0; removed to clean up the config surface. No runtime
  impact — consumer-group polling cadence is driven by
  heartbeat/session-timeout, which remain configurable.

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
