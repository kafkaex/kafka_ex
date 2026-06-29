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

### Consumer `auto_offset_reset` default

The default `auto_offset_reset` is now `:latest` (matching the Kafka/Java client
default), changed from `:none`. This only affects a consumer that has **no valid
committed offset** and does **not** set `auto_offset_reset` explicitly. The old
default behaved differently in the two cases that trigger a reset:

- **New consumer group (no committed offset):** previously started silently from
  the **earliest** offset, replaying the whole topic. Now starts from the
  **latest** offset, consuming only messages produced after it joins.
- **Committed offset out of range:** previously **raised**. Now resets to the
  **latest** offset.

`:none` is still available and is now consistent across both cases — it
**raises** instead of guessing, so a missing or out-of-range offset surfaces
loudly rather than silently replaying or skipping data. To replay from the
beginning (the old new-group behavior), set `:earliest` explicitly; to keep the
old strict raise-on-bad-offset behavior, set `:none`:

```elixir
config :kafka_ex, auto_offset_reset: :earliest
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

> **Option key:** the broker list option is `:brokers` (matching the
> `config :kafka_ex, brokers:` key). The pre-1.0 `:uris` option is still
> accepted as a deprecated alias. `start_client/1` now merges `config.exs`
> defaults automatically, so you no longer need `KafkaEx.build_worker_options/1`
> just to inherit your configured brokers and consumer group.

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

### Consumer-group heartbeat crash-loop is now bounded (new in 1.1.0)

A consumer-group member whose heartbeat process keeps dying abnormally (an
uncatchable `:kill`, OOM, or unstructured crash) used to rejoin **forever** —
silent, consuming nothing, never alerting. It is now bounded: more than
`crash_rejoin_max_restarts` such crashes within `crash_rejoin_window_ms`
(defaults `10` / `60_000` ms) stops the member terminally and emits
`[:kafka_ex, :consumer, :member_terminated]` with `{:crash_loop, _}`.

**What you must check:**

- **If you start `ConsumerGroup` under your own supervisor** (the common case,
  and what the docs recommend), there is nothing to do — on a crash-loop trip the
  member stops and your supervisor restarts the whole `ConsumerGroup` fresh under
  your restart strategy. Size your supervisor's `max_restarts`/`max_seconds` to
  absorb a correlated kill wave (e.g. a fleet-wide OOM/deploy) where many members
  can trip at nearly the same time.

- **If you start `ConsumerGroup` standalone** (not under a supervisor), a
  crash-loop trip now **permanently stops consumption** instead of looping. Wrap
  it in a supervisor, or set `crash_rejoin_max_restarts: :infinity` to keep the
  unbounded rejoin (no terminal give-up):

  ```elixir
  # loop forever on abnormal heartbeat crashes (unbounded rejoin, no give-up)
  config :kafka_ex, crash_rejoin_max_restarts: :infinity

  # or per consumer group
  KafkaEx.Consumer.ConsumerGroup.start_link(
    MyConsumer, "my-group", ["topic"],
    crash_rejoin_max_restarts: :infinity
  )
  ```

To tune rather than disable, raise the restart **count** rather than widening the
window (a wider window accumulates more crashes and trips more eagerly).

### Static consumer-group membership (new in 1.1.0)

`KafkaEx.Consumer.ConsumerGroup` now supports KIP-345 static membership via a
new `:group_instance_id` option. **This feature is entirely opt-in — default
behavior is unchanged.**

When `:group_instance_id` is set, the member presents a stable instance id on
JoinGroup/SyncGroup/Heartbeat. The broker retains its partition assignment
across a restart (until `session.timeout.ms`) instead of triggering a rebalance.
In addition, **LeaveGroup is suppressed on shutdown** — this is a behavior
change that applies **only when `:group_instance_id` is set**; dynamic members
(the default) continue to send LeaveGroup as before.

**Uniqueness is mandatory.** Each member in the group must have a distinct
`:group_instance_id`. If two members share an id, the broker fences the later
arrival (`:fenced_instance_id` → terminal stop, `member_terminated` telemetry
with `terminal_class: :fenced`). Common footgun: setting a bare string in
`config.exs` — every replica shares the same value, and all but one get fenced.
Derive the id per node instead:

```elixir
# Good: resolved at runtime, unique per pod/node
group_instance_id: System.get_env("POD_NAME") || (node() |> to_string())

# Or via an MFA tuple (resolved lazily by ConsumerGroup):
group_instance_id: {MyApp.Kafka, :instance_id, []}

# Bad: every replica shares this string → all but one FENCED
group_instance_id: "my-consumer"
```

**Raise `session_timeout` to cover your restart/deploy window.** While the
assignment is held for `session.timeout.ms` after the last heartbeat, a restart
that takes longer than this window will cause the broker to expire the session
and rebalance anyway. The broker default is typically 45 s; set it higher if
your deployment can take longer:

```elixir
# Per consumer group (preferred)
KafkaEx.Consumer.ConsumerGroup.start_link(
  MyConsumer, "my-group", ["topic"],
  group_instance_id: System.get_env("POD_NAME"),
  session_timeout: 120_000   # 2 minutes — cover your deploy window
)
```

Requires Kafka >= 2.3. A too-old broker logs a warning and the consumer runs
with dynamic membership (no error, no crash).

### `KafkaEx.API.start_client()` is now unnamed by default

Pre-v1.0 patch — and the v1.0 release prior to this fix — `start_client()`
with no `:name` registered the resulting GenServer as `KafkaEx.Client`.
The `:name` option was documented but silently ignored.

After this change, `start_client()` (no `:name`) returns `{:ok, pid}` and
does NOT register globally. To get the old behavior, pass `:name` explicitly:

```elixir
# Old implicit behavior:
{:ok, _} = KafkaEx.API.start_client()
KafkaEx.API.metadata(KafkaEx.Client)  # used to work, no longer registered

# New explicit behavior:
{:ok, _} = KafkaEx.API.start_client(name: KafkaEx.Client)
KafkaEx.API.metadata(KafkaEx.Client)  # works
```

The `:name` option now also accepts `{:global, term}` and `{:via, mod, term}`
shapes for distributed / registry-based registration.

Note: most users bind the returned pid (`{:ok, client} = start_client(...)`)
and pass it to subsequent API calls. Those callers are unaffected.

### Fetch timeouts now derive from `:max_wait_time`

Previously `KafkaEx.API.fetch/5`'s GenServer.call timeout (5s default)
and the per-broker Socket.recv timeout (1-3s from `sync_timeout` app
config) were not aligned with the `:max_wait_time` option (default
10s). At logend the broker would hold the long-poll for `max_wait_time`
ms; both upper timeouts fired first, causing socket close + retry
loops until the request was abandoned. Symptom: streams built on
`KafkaEx.Consumer.Stream` or direct `KafkaEx.API.fetch/5` calls
hanging or returning unexpected errors when the consumer caught up
to logend.

`KafkaEx.API.fetch/5` now derives both timeouts automatically:

- `network_timeout = max_wait_time + 5_000` (passed via opts to the
  Client GenServer, used as the Socket.recv timeout).
- `call_timeout = network_timeout × 3 + 5_000` (used as the
  GenServer.call timeout). The `× 3` multiplier covers the Client's
  retry budget (`@retry_count = 3`) so the call does not exit while
  retries are still in flight, which would leak a dead-mailbox reply.

With the defaults this is `max_wait_time = 10s`, `network_timeout =
15s`, `call_timeout = 50s`. The high call_timeout is a worst-case
ceiling, not a typical wait — a healthy fetch returns in `network_timeout`
or less; only the retry path approaches `call_timeout`. If you need
a tighter ceiling, pass a smaller `:max_wait_time` (or pass an
explicit `:network_timeout` opt).

If you bumped `:kafka_ex, :sync_timeout` upward as a workaround, you
can revert to the default. The `sync_timeout` config still governs
non-fetch requests (metadata, find_coordinator, etc.) where it works
correctly.

If you want fast logend halt (e.g. for short-lived streams), pass
`max_wait_time: 1_000` (or less) explicitly. The derived timeouts
will follow.

### `KafkaEx.Consumer.Stream` halts on fetch errors

Paired with the timeout fix: when a fetch underlying a stream returns
`{:error, _}`, `KafkaEx.Consumer.Stream` now terminates cleanly via
`{:halt, offset}` and emits a `Logger.warning`:

```
Stream halting after fetch error on <topic>/<partition> at offset <offset>: <error_code>
```

Previously, with `no_wait_at_logend: false` (default) the stream
silently looped on a wildcard fallback in `stream_control/3`, never
yielding events and never surfacing the error. With `no_wait_at_logend:
true` the stream halted via the same wildcard but emitted no log.

Operators can grep `"Stream halting after fetch error"` to detect
halts, or attach to `[:kafka_ex, :fetch, :stop]` telemetry with
`metadata.result == :error` to alert on the underlying fetch failure
(which fires regardless of stream consumption).

A related crash class is also fixed: `auto_commit: true` plus a fetch
error response previously raised `KeyError` on `fetch_response.message_set`.
`need_commit?/2` now returns false for any error response, so no
commit is attempted and the stream halts cleanly.

This halt applies to **transient** errors too — fetch timeouts,
connection drops, and leader changes reach the stream only after the
client's internal retries (3 attempts, with a metadata refresh on
leadership errors) are exhausted. The stream itself does not retry; it
halts and leaves recovery to you. (Reference clients such as the Java
consumer, librdkafka, and brod retry transient errors inside the
consumer loop; KafkaEx's lazy `Stream` delegates that to a supervisor
instead. Richer in-stream recovery is tracked for a future release.)

Migration: if your application relied on the silent-spin behavior to
"wait through" transient errors, wrap your stream construction in a
supervisor or `Task` restart loop that re-enters the stream after the
warning fires. Track the offset of the last message you successfully
process and re-enter from the next one:

```elixir
%KafkaEx.Consumer.Stream{client: client, topic: topic, partition: p,
  offset: last_processed_offset + 1, ...}
```

Re-entering from the *original* stream struct restarts from its initial
`:offset` and reprocesses everything consumed so far (safe under
at-least-once if your handler is idempotent, but usually not what you
want).

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
