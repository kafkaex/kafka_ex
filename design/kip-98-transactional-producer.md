# KIP-98 Design — Idempotent & Transactional Producer (kafka_ex v1.1.0)

**Status:** Design — second brainstorming pass + six-lens review complete. **Two decisions open** (see §11).
**Branch:** `transactional-producer`
**Author/maintainer:** Piotr Rybarczyk (@Argonus)
**Supersedes:** the original spec `2026-05-28-kip-98-design.md` wherever they conflict. This file is authoritative.

---

## How to use this document (read first)

This is a **handoff spec**. It is self-contained: a fresh agent or engineer should be able to pick it up without the conversation that produced it. It has three layers:

1. **§1 Scope** + **§2 Decision Log** — the *history*: every decision, what it changed, and *why*. Read this to understand intent.
2. **§3–§9 The Spec** — the *current design*: what to build, incorporating every accepted review fix.
3. **§10 Review synthesis**, **§11 Open decisions**, **§12 PR plan**, **§13 Ledger** — disposition of the six expert reviews, the two unresolved choices, and the build sequence.

When you resume: **start at §11** (two open decisions), resolve them with the maintainer, then fold the result into §3–§9 and §12, and begin the PR stack.

This file lives at `design/kip-98-transactional-producer.md` — a tracked location chosen deliberately because `docs/` and `doc/` are gitignored as ExDoc generated output. It is meant to be committed alongside the PR stack as the design-of-record; it is not shipped in the published Hex docs.

---

## 1. Scope

**In scope for v1.1.0:**
- Idempotent producer (opt-in via `enable_idempotence: true`)
- Transactional producer — Java-style primitives + Elixir block wrapper, **reusable manual lifecycle** (see Decision Y1→1B)
- Consume-process-produce via `add_offsets/3` (dynamic membership only; see honesty caveat §7)

**Deferred to v1.2.0 (needs a Kayrock V3 bump):**
- KIP-360 graceful `UnknownProducerId` recovery — needs InitProducerId **V3** request fields (`producer_id`/`producer_epoch`)
- KIP-588 `transaction.timeout.ms` recovery — same V3 requirement
- Static-membership-safe `add_offsets/4` — needs TxnOffsetCommit **V3** (`member_id`/`generation_id`/`group_instance_id`, KIP-447)

**Deferred to v2.0.0:** KIP-679 idempotence default-ON (behaviour change → major bump).

**Not in scope:** KIP-664 admin tooling (DescribeTransactions/ListTransactions/AbortTransaction); KIP-848 new rebalance protocol; KIP-368 SASL re-auth; **KIP-939 two-phase commit / `PREPARED_TRANSACTION`** (bleeding-edge, newer wire versions).

**Kayrock readiness (verified against `/Users/piotr.r/Projects/kayrock/lib/generated/`):**
InitProducerId **V2**, AddPartitionsToTxn **V1**, AddOffsetsToTxn **V1**, EndTxn **V1**, TxnOffsetCommit **V2**, FindCoordinator **V3**. Sufficient for basic KIP-98.
- InitProducerId V2 request carries `transactional_id` + `transaction_timeout_ms` + tagged_fields, and **no** `producer_id`/`producer_epoch` request fields → we **can** expose `transaction.timeout.ms` in v1.1.0, but cannot do KIP-360 PID-preserving epoch bumps (those need V3).
- FindCoordinator V1+ carries `key_type` (`:int8`); kafka_ex already maps `:transaction → 1` (`lib/kafka_ex/protocol/kayrock/find_coordinator/request_helpers.ex:55-62`).
- `Kayrock.RecordBatch` struct has `producer_id`/`producer_epoch`/`base_sequence` (default `-1`) and serializes them into the batch header (`/Users/piotr.r/Projects/kayrock/lib/kayrock/record_batch.ex:46-59,135-138`); attribute bit 4 = transactional, bit 5 = control.

---

## 2. Decision Log (history + rationale)

Each entry: the decision, the alternatives considered, why this one, and what it supersedes. IDs are referenced throughout the spec.

### 2.1 State ownership
- **A2 — single shared ETS keyspace.** One `KafkaEx.Client.IdempotentState` ETS table, owned by the Client, keyed `{producer_id, topic, partition} → next_sequence`, serving **both** idempotent and transactional produces. *Alternatives:* plain map on the Transaction gen_server (A1); per-Transaction ETS (A3). *Why:* one mechanism, atomic `:ets.update_counter/4`, no second counter store. *Supersedes:* original spec left transactional sequence state unspecified.
- **C1 — Client owns the wire path.** Transaction gen_server never touches ETS or builds requests; it delegates `Client.transactional_produce(client, producer_id, epoch, topic, partition, messages, opts)`. *Why:* keeps socket/correlation-id/state mutation serialized in the one process that owns it; preserves "Transaction holds no sockets."
- **E2 (modified by 1B) — cleanup at the lifecycle boundary, monitor-backed.** ETS rows for a `producer_id` are cleared when its Transaction **terminates or its epoch is bumped** — *not* on commit-returns-to-`:ready` (see 1B). Cleanup is driven by a **Client `Process.monitor` on each Transaction**, deleting on `:DOWN`, because `terminate/2` is not guaranteed to run (review finding R-OTP-1). `terminate/2` remains as the graceful fast path. *Supersedes:* original E2 (terminate-only).
- **F2 — idempotence flag and transactions are orthogonal.** `enable_idempotence` governs only non-transactional Client produces; transactional produces are always EOS regardless. *Why:* separate gen_servers, separate ETS keyspaces, separate wire paths — no coupling, unlike Java where one producer does both.

### 2.2 Retry payload stability
- **G2 — three structural layers.** (A) typed `KafkaEx.Producer.Request` struct carrying the built Kayrock term + wire-final fields; (B) single construction site `RequestBuilder.produce_request/2` (narrowed return type, only caller of `make_ref/0`); (C) integration test asserting byte-identical wire payload across a simulated retry. *Alternatives:* original "5-layer defense" (G1) and test-only (G3). *Why:* layers 1 (comment/Credo) and 3 (downstream signature) in the original were restatements of A+B; folded into moduledoc. Today's code is already correct (`client.ex:596` builds once, retry reuses by reference) — this is **regression protection**, not a live-bug fix.

### 2.3 Producer directory cleanup (prerequisite)
- **R1 — standalone prerequisite PR.** Reorganize `lib/kafka_ex/producer/` so file paths mirror module paths (matching `lib/kafka_ex/network/`). Pure `git mv` + rename, zero behaviour change.
- **S1 — accept the public break.** Rename behaviour module `KafkaEx.Producer.Partitioner` → `KafkaEx.Producer.Partitioner.Behaviour`. Users with custom partitioners change one line. *Alternatives:* compat shim (rejected — two ways forever); keep file/module mismatch (rejected). Document in CHANGELOG.

### 2.4 Transaction lifecycle & state machine
- **Y1 → 1B — reusable manual lifecycle.** Base model is brod-style (commit terminates the gen_server). **1B refinement:** the **manual lifecycle (Form 2)** is *reusable* — `commit_transaction/1` returns the gen_server to `:ready` instead of terminating, and the next transaction runs on the **same `(producer_id, producer_epoch)`**. The **block wrapper** and **one-shot** paths still terminate on commit. *Alternatives:* pure Y1 (1A — keep brod model, document per-transaction `InitProducerId` cost). *Why 1B:* three reviewers (Java/Python/JS) flagged that pure Y1 pays a `FindCoordinator`+`InitProducerId`+epoch-bump RTT per transaction, vs Java/librdkafka which init once and reuse; the hot CTP/Broadway loop shouldn't pay that per batch. *Cost of 1B:* re-introduces `:ready` and the across-transaction sequence-continuity rule (§2.4 below, and §1.5-equivalent in §4).
- **T2 (modified by 1B) — collapsed state machine, `:ready` retained for reuse.** Drop `:committing`/`:aborting`/`:transaction_busy` (unreachable under a synchronous gen_server — a blocked `handle_call` can't be observed mid-flight). Keep `:ready` (1B reuse needs it). *Caveat:* the collapse of `:committing`/`:aborting` depends on commit/abort being **synchronous** `handle_call`s; if ever made async, they return.
- **U1 — block wrapper does not trap exits; Transaction gen_server does.** The block wrapper runs on the *caller* process and must **not** `Process.flag(:trap_exit, true)` (it would clobber the caller's flag). Uncatchable caller kills are handled by the broker's `transaction.timeout.ms`. **However** (review finding R-BROD-1), the **Transaction gen_server itself traps exits** so its `terminate/2` can abort proactively (brod's model) — this is a different process from the wrapper, so no contradiction. *Supersedes:* original framing that leaned on broker timeout for *all* abort cases; ordinary failures now abort proactively via trapped terminate.

### 2.5 API surface
- **Two forms + sugar.** Block wrapper `transaction(client, "tx-id", fn t -> ... end)` (explicit id, managed) and manual lifecycle (`init_transaction/2` … `commit/abort/close`). *Rejected:* auto-id `transaction(client, fn)` (zombie-fencing footgun — §5.5); `begin_transaction/1` was dropped under pure Y1 but **returns under 1B** (see open Decision 1); `with_one_shot_transaction` (the explicit-id wrapper already covers one-shot).
- **AA1 — `use KafkaEx.API, transactional_id:` sugar.** Generates `transaction/1` that calls the block wrapper. **Caches only the compile-time id string — never a pid** (review finding R-OTP-2; a cached pid would contradict the lifecycle and be a concurrency hazard). Document: one `transactional_id` per *running instance*, never shared across replicas.
- **`rollback/2`** — Ecto-style early-exit abort from inside a block body. No process-dictionary magic; `t` is always passed explicitly.

### 2.6 acks enforcement & rename
- **Z1 (refined) — reject only on explicit conflict.** Rename `:required_acks` → `:acks` (deprecation alias + once-per-process warning). When `enable_idempotence: true`, return `{:error, :idempotence_requires_acks_all}` if `:acks` was **explicitly** set to anything but `-1`; an **unset** `:acks` defaults to `-1` silently (matches librdkafka's coerce-when-silent / error-on-conflict; review finding R-PY-2). No raises (Elixir `start_link` convention). *Supersedes:* original contradiction (§9.2 raise vs §6.3 error tuple) and the pure-reject-always reading.

### 2.7 Consume-process-produce
- **AB1 — ship `add_offsets/3` on TxnOffsetCommit V2; document the gap.** No runtime refusal of static membership. *Alternatives:* runtime-detect-and-refuse (AB2 — costs a `DescribeGroups` RTT, stale anyway); don't ship CTP (AB3 — cuts the headline feature). *Why:* matches brod's posture. **Honesty correction (review R-PY-C2/R-KAFKA-I2):** V2 fences on `producer_epoch` only; it is safe for dynamic membership **only under the legacy one-`transactional.id`-per-input-partition pattern**. A single shared id + dynamic rebalance still has a generation-fencing gap until v1.2.0. The CHANGELOG/docstring must say this.

### 2.8 Resolved open items (were O1/O2; closed by unanimous six-lens consensus)
- **O1 → identity-only `built_at_ref`, no raising tripwire.** All six reviewers agreed; brod/librdkafka achieve retry stability structurally (advance the sequence only on success). The G2 integration test is the guard. The Elixir reviewer noted a tripwire is impossible to even write under the current single-build retry loop.
- **O2 → drop the 249-byte `transactional_id` check.** Java, brod, and librdkafka do **no** client-side length check (249 is the *topic-name* rule). Keep non-empty + no-control-chars; the broker enforces length.

---

## 3. State ownership & idempotent sequence store (the spec)

`KafkaEx.Client.IdempotentState` — ETS table created and owned by the Client.
- **Table options:** `:set`, **`:protected`** (Client is the sole writer — all sequence mutations funnel through the Client process per C1, so single-writer is correct and `write_concurrency` buys nothing), `read_concurrency: false`. The opaque-struct wrapper around the tid is documentation-of-intent; the **real guard is the `:protected` Client ownership**.
- **Key:** `{producer_id, topic, partition}` → `next_sequence` (the *returned* producer_id from InitProducerId — do not assume PID stability across re-init under V2).
- **`consume_sequence/3`** (`{state, topic, partition}`) is the only path to obtain-and-advance a sequence; uses `:ets.update_counter/4` and **advances only on the build path**, with the broker-ack semantics that a failed send does not double-advance (so retries reuse the same `base_sequence` — see §4 retry rule).
- **Cleanup (E2):** the Client `Process.monitor`s each Transaction at `init_transaction`; on `:DOWN` (any reason, including `:kill`/`:brutal_kill` which skip `terminate/2`) it `:ets.match_delete`s `{{producer_id, :_, :_}, :_}`. Also cleared on epoch bump (re-init recovery). **Not** cleared on commit-returns-to-`:ready` (1B reuse keeps the keyspace alive for the `(pid, epoch)`).
- Real `@moduledoc`/`@doc` (no `@moduledoc false`).

**Sequence-continuity rule (load-bearing under 1B):** within a `(producer_id, producer_epoch)`, `base_sequence` is monotonic and gapless across **all** batches **and across transactions** (reuse runs many transactions on one epoch). It resets to 0 **only** on an epoch change (new `InitProducerId`). A retry of the same produce MUST reuse the same `base_sequence` (enforced by §4 / G2). `OUT_OF_ORDER_SEQUENCE_NUMBER` from the broker indicates a violation.

---

## 4. Retry payload stability (the spec)

New module `KafkaEx.Producer.Request` (see §6 dir layout):
```elixir
defmodule KafkaEx.Producer.Request do
  @moduledoc """
  Built, retry-safe Produce request. Wire-final fields (producer_id,
  producer_epoch, base_sequence, transactional_id) MUST NOT change across
  retries — the broker dedups on (producer_id, producer_epoch, base_sequence).
  Constructed only by KafkaEx.Client.RequestBuilder.produce_request/2.
  """
  @enforce_keys [:built_at_ref, :request]
  defstruct [:built_at_ref, :producer_id, :producer_epoch,
             :base_sequence, :transactional_id, :request]

  @type t :: %__MODULE__{
          built_at_ref: reference(), producer_id: integer() | nil,
          producer_epoch: integer() | nil, base_sequence: integer() | nil,
          transactional_id: String.t() | nil, request: term()}
end
```
- **Construction site:** `RequestBuilder.produce_request/2` returns `{:ok, KafkaEx.Producer.Request.t()}` (narrowed from `{:ok, term}`); only place that stamps `built_at_ref` via `make_ref/0`. `built_at_ref` is **identity-only** (O1 resolved — no runtime tripwire).
- **Unwrap once** in `handle_produce_request` (`%Request{request: inner}`); the generic `handle_request_with_retry` loop is unchanged and reuses `inner` by reference.
- **Layer C test:** simulate mid-produce wire failure; assert retry bytes byte-identical.

**`max.in.flight ≤ 5` / `retries > 0` (review R-JAVA-C1 / R-KAFKA-I4):** KIP-98 ordering requires ≤5 in-flight unacked requests per partition. kafka_ex produce is a **synchronous `GenServer.call` that blocks on the broker ack** (verified `client.ex:592`→`handle_request_with_retry`) — effectively 1 in-flight per partition — so the constraint holds **by construction**. Document this as a load-bearing invariant; any future "concurrent partition dispatch" refactor (hinted in G2) MUST preserve ≤5 and MUST keep `base_sequence` stable. Add a `retries > 0` check alongside the acks check in PR 1 (idempotence with zero retries is a Java `ConfigException`).

---

## 5. Transaction lifecycle, state machine & API (the spec)

### 5.1 State machine (`KafkaEx.Producer.Transaction.StateMachine`, pure, table-tested)
```
:uninitialized   — pre-InitProducerId (transient; init in start_link, caller never observes)
:ready           — initialized, no open transaction (1B reuse target)
:in_transaction  — produce / add_offsets / commit / abort allowed
:abortable       — recoverable error; commit refused (returns {:error, :must_abort, last_cause}); abort allowed
:fatal           — fenced/unrecoverable; only close (best-effort local teardown)
```
Transitions:
- `:uninitialized --InitProducerId ok--> :ready`
- `:ready --begin--> :in_transaction` (Decision 1 governs whether begin is explicit or implicit)
- `:in_transaction --commit ok--> :ready` (1B: alive) | block-wrapper/one-shot: **terminate**
- `:in_transaction --abort ok--> :ready` (1B) | wrapper/one-shot: terminate
- `:in_transaction --recoverable error--> :abortable`
- `:abortable --abort--> :ready` (1B) | terminate
- `commit from :abortable` → `{:error, :must_abort, last_cause}` (no wire call)
- any `--fence/unrecoverable--> :fatal`; in `:fatal`, abort is **local teardown only** — do **not** send `EndTxn` after a fence (epoch is stale, broker rejects). Surface a distinct `:producer_fenced` reason (review R-JS-M4), not a generic `:fatal`, so workers know to stop rather than retry.
- `close` from any state → terminate + clear ETS.

### 5.2 Error taxonomy (`KafkaEx.Producer.Transaction.ErrorClassifier`, first-class table, PR 0)
Three classes (librdkafka's predicate model; review R-JAVA-I1 / R-PY-C1 / R-KAFKA-I3):
- **`:retriable`** (re-drive the *same* control call, same `base_sequence`): `NOT_COORDINATOR`, `COORDINATOR_NOT_AVAILABLE`, `COORDINATOR_LOAD_IN_PROGRESS`, `CONCURRENT_TRANSACTIONS`, `NOT_ENOUGH_REPLICAS`, `NOT_LEADER_OR_FOLLOWER`, `REQUEST_TIMED_OUT`.
- **`:abortable`** (abort txn; under 1B may then begin again; degraded KIP-360 recovery = abort→re-init→new epoch): `UNKNOWN_PRODUCER_ID`, `OUT_OF_ORDER_SEQUENCE_NUMBER`, `INVALID_TXN_STATE`, `TOPIC_AUTHORIZATION_FAILED`, `GROUP_AUTHORIZATION_FAILED`, exhausted-retriable. **`UNKNOWN_PRODUCER_ID` / `OUT_OF_ORDER_SEQUENCE` are abortable, NOT fatal** — this is the recovery path Y1/1B handles cleanly.
- **`:fatal`** (close producer): `INVALID_PRODUCER_EPOCH`/`PRODUCER_FENCED`, `TRANSACTIONAL_ID_AUTHORIZATION_FAILED`, `CLUSTER_AUTHORIZATION_FAILED`, `UNSUPPORTED_VERSION`, `INVALID_PRODUCER_ID_MAPPING`.

Retriable control-call errors stay in `:in_transaction` (or a transient retry) and re-drive up to a bounded count/deadline before escalating to `:abortable`.

### 5.3 API forms
**Form 1 — block wrapper (managed, terminates on commit):**
```elixir
KafkaEx.API.transaction(client, "orders-worker-1", fn t ->
  KafkaEx.API.produce(t, "out-topic", 0, msg)
  KafkaEx.API.add_offsets(t, "my-group", offsets)
end)   # => {:ok, result} | {:error, reason}
```
**Form 2 — manual, reusable (1B):**
```elixir
{:ok, tx} = KafkaEx.API.init_transaction(client, "orders-worker-1")  # InitProducerId once
# ... per Decision 1, either begin_transaction(tx) or implicit-on-first-produce ...
KafkaEx.API.produce(tx, "out-topic", 0, msg)
KafkaEx.API.commit_transaction(tx)    # 1B: tx returns to :ready, ALIVE
# ... reuse on same epoch ...
KafkaEx.API.commit_transaction(tx)
KafkaEx.API.close_transaction(tx)     # explicit teardown → terminate + clear ETS
# KafkaEx.API.abort_transaction(tx)   # also available; 1B: returns to :ready
```
**Form 3 — `use` sugar (AA1):**
```elixir
defmodule MyApp.Kafka do
  use KafkaEx.API, client: MyApp.KafkaClient, transactional_id: "orders-worker-1"
end
MyApp.Kafka.transaction(fn t -> MyApp.Kafka.produce(t, "topic", 0, msg) end)
```

**Dispatch + nil-partition (review R-OTP-1b):** `KafkaEx.API.produce(t, topic, partition, msg)` works whether `t` is a Client or Transaction pid (both `GenServer.server()`, both handle `{:produce, ...}`). But `produce(_, _, nil, _, _)` resolves the partition via `topics_metadata` (`api.ex:719`→`resolve_partition`→`topics_metadata`), a *different* call. The Transaction gen_server therefore **forwards metadata/read calls to its Client** (consistent with C1). Document that transactional produce supports `nil` partition via this delegation.

**Indeterminate commit-outcome (review R-JAVA-I3):** if the gen_server dies after `EndTxn` is dispatched but before the reply reaches the caller, the caller's `commit_transaction/1` exits/`:noproc` while the broker may have committed. This ambiguity is inherent to Kafka (Java has it too). Document it and add a test for "gen_server dies after EndTxn dispatched, before reply."

### 5.4 Block-wrapper exit semantics (U1)
Wrapper uses `try ... rescue / catch :exit`; does **not** trap. (The *Transaction gen_server* traps — §2.4.)

| Body outcome | Wrapper action |
|---|---|
| normal return | commit → `{:ok, result}` |
| `raise` | abort, re-raise. **If the abort itself also fails, wrap both** in `KafkaEx.Producer.Transaction.AbortFailedError{original, abort_error}` (review R-JS-I2) — don't drop the abort failure. |
| `throw` | abort, re-throw |
| `KafkaEx.API.rollback(t, reason)` | abort → `{:error, reason}` |
| `exit(:normal)` (called in body) | **caught** via `catch :exit, :normal`; issue the abort there (the abort is what prevents a half-finished commit — the re-exit is cosmetic since `:normal` doesn't propagate across links). |
| `GenServer.call` timeout (`exit({:timeout,_})`) | abort, re-exit |
| linked child of body dies | shares caller's fate; commit/death ordering is a race resolved by `transaction.timeout.ms`. Recommend `Task.Supervisor`/unlinked tasks inside a tx body. |
| caller hard-kill (uncatchable) | wrapper+tx die; broker aborts via `transaction.timeout.ms` |

Document a `try/after` cleanup recipe (no `finally` hook in the wrapper).
**`transaction.timeout.ms` is settable** in v1.1.0 (InitProducerId V2 carries `transaction_timeout_ms`); expose it at `init_transaction`/producer config. Default = broker default (60s).

### 5.5 Rejected API forms (with reasons)
- **Auto-id `transaction(client, fn)`** — defeats zombie fencing for multi-instance deployments (each replica gets a different id → broker can't fence the zombie → duplicate writes). brod offers an auto-id, **but only in its CTP processor's subscriber init, not its manual API** (review R-BROD-I3); its manual `transaction/3` requires an explicit id, same as ours.
- **`with_one_shot_transaction`** — the explicit-id block wrapper already is one-shot.

---

## 6. Producer directory cleanup (the spec, R1+S1)
Target layout (mirrors `lib/kafka_ex/network/`):
```
lib/kafka_ex/producer/
  partitioner/
    behaviour.ex   → KafkaEx.Producer.Partitioner.Behaviour   (was producer/behaviour.ex)
    default.ex     → KafkaEx.Producer.Partitioner.Default
    legacy.ex      → KafkaEx.Producer.Partitioner.Legacy
  request.ex       → KafkaEx.Producer.Request        (new, §4)
  transaction.ex   → KafkaEx.Producer.Transaction    (new, §5)
  transaction/
    state_machine.ex → KafkaEx.Producer.Transaction.StateMachine
    error_classifier.ex → KafkaEx.Producer.Transaction.ErrorClassifier
```
Prerequisite PR, pure `git mv` + rename, CHANGELOG. Public break (S1) documented.

---

## 7. Wire choreography (the spec — was the biggest review gap, R-KAFKA-C1/C2/C3)

The design body previously named only `TxnOffsetCommit`. Full EOS choreography, with coordinator routing:

**Coordinator routing:** `InitProducerId`, `AddPartitionsToTxn`, `AddOffsetsToTxn`, `EndTxn` → **transaction coordinator** (`FindCoordinator(key=transactional_id, key_type=1)`). `TxnOffsetCommit` → **group coordinator** (`FindCoordinator(key=group_id, key_type=0)`). So `add_offsets/3` triggers a group-coordinator `FindCoordinator` in addition to the cached txn coordinator.

**Produce path:**
1. `InitProducerId(transactional_id, transaction_timeout_ms)` → `{producer_id, producer_epoch}` (once per producer under 1B).
2. On **first** produce to a given `{topic, partition}` within the current transaction: `AddPartitionsToTxn(producer_id, producer_epoch, [partition])` to the txn coordinator, **before** the `Produce`. Transaction tracks the "partitions already enrolled" set to avoid re-adding. `AddPartitionsToTxn` may return `CONCURRENT_TRANSACTIONS` (retriable, common right after init).
3. `Produce` (acks=-1) to the partition leader. **RecordBatch must populate header `producer_id`/`producer_epoch`/`base_sequence` AND OR bit 4 (transactional) into attributes** — `request_helpers.ex` currently sets only compression, leaving the three header fields at `-1`, which the broker rejects. New helper `attributes_for_record_batch(compression, opts)` + header population. (Idempotent-only batches set the same header fields + bit 4 minus the transactional_id — applies to PR 1 too.)

**Offsets path (`add_offsets/3`):**
4. `AddOffsetsToTxn(producer_id, producer_epoch, group_id)` to the **txn coordinator** — enrolls the `__consumer_offsets` partition in the transaction. **MUST precede** step 5.
5. `TxnOffsetCommit(...)` to the **group coordinator**.
(brod does exactly this two-step in `send_cg_and_offset`; review R-BROD confirms.)

**Commit/abort:**
6. `commit → EndTxn(transaction_result: true)`; `abort → EndTxn(transaction_result: false)` to the txn coordinator, carrying current `producer_id`/`producer_epoch`. `transaction_result` is a **boolean** on the wire (not 1/0).

**Coordinator failover (review R-OTP-I4):** on `:not_coordinator` the **Client** (not the Transaction — C1) clears the `transaction_coordinators` slot, re-runs `FindCoordinator`, retries once. Mirror the existing `update_consumer_group_coordinator` pattern (`client.ex:1056`). Note: `ClusterMetadata` currently has `put_consumer_group_coordinator/3` but no `clear_*`; add `clear_transaction_coordinator/2` (and the txn `put_`/`NodeSelector.transaction_coordinator/2`).

---

## 8. Consume-process-produce & static-membership honesty (the spec, AB1)

`add_offsets/3` (dynamic membership only in v1.1.0; no opts arg — V2 can't carry membership fields). `add_offsets/4` arrives in v1.2.0 with the Kayrock V3 bump (KIP-447 parity, carrying `group_instance_id`/`member_id`/`generation_id`).

**Honesty caveat (CHANGELOG + docstring, review R-PY-C2/R-KAFKA-I2/R-JS-I1):** TxnOffsetCommit V2 fences only on `producer_epoch`. It is safe for dynamic membership **only under the legacy one-`transactional.id`-per-input-partition pattern**; a single shared id with dynamic rebalancing retains a narrow generation-fencing gap (a zombie that produces with a still-valid epoch after a rebalance generation change can't be fenced on generation grounds) until v1.2.0. Static membership is strictly worse (unsupported). Frame as "pre-KIP-447 semantics" — current Java no longer exposes a metadata-less overload; KafkaJS's `sendOffsets` shares this exact gap (so AB1 matches both brod and KafkaJS).

**GenConsumer `{:no_commit, state}` (review R-BROD-I4):** advance **all three** offsets (`current`/`acked`/`committed`) to `last_offset + 1`; library does **not** call `OffsetCommit`; terminate's `acked == committed → nothing to commit` short-circuit fires. Reset the async-commit `last_commit` timer. **Add a "consumer-managed" mode switch** (brod's `offset_commit_policy: consumer_managed`) so the library's auto-commit timer does **not** race the transactional offset commit, and specify the **offset-bootstrap path** on startup/rebalance (kafka_ex equivalent of brod's `get_committed_offsets/3` — read committed offsets from the broker to set fetch positions). Expose `set_offset/2` (manual rewind on abort) and `force_commit/1`.

---

## 9. acks enforcement & RecordBatch bit (the spec)
- **Schism:** `request_helpers.ex:32` reads `:acks` (wire default `-1`); `client.ex:581` reads `:required_acks` (telemetry only); `api.ex` docs falsely claim default `1`. Rename `:required_acks` → `:acks` (PR 0), deprecation alias + once-per-process warning.
- **Z1 (refined):** `enable_idempotence: true` + **explicitly-set** `:acks ≠ -1` → `{:error, :idempotence_requires_acks_all}` (boot + per-call, no raise). Unset `:acks` → defaults to `-1` silently. Reason: with `acks ≠ -1` a leader can ack then die pre-replication, losing the write while the sequence advances.
- **`is_transactional` bit:** covered in §7 step 3.

---

## 10. Six-lens review synthesis

All six returned **"With fixes"**; none found the architecture wrong. Disposition:

| Lens | Headline catches | Disposition |
|---|---|---|
| **Elixir/OTP** | `terminate/2` ETS cleanup leaks on caller-crash → use Client monitor (R-OTP-1); AA1 "cache pid" contradicts lifecycle → cache id string only (R-OTP-2); coordinator retry must live in Client not Transaction (R-OTP-I4); nil-partition produce needs metadata delegation (R-OTP-1b). | **All folded** (§3, §2.5, §7, §5.3). |
| **Kafka protocol** | `AddOffsetsToTxn` + `EndTxn` absent from flow (C1/C2); RecordBatch header fields unset (C3); error table incl. `OUT_OF_ORDER`/`CONCURRENT_TRANSACTIONS` (I3); dynamic-on-V2 overstated (I2); `max.in.flight≤5` (I4). | **All folded** (§7, §5.2, §8, §4). |
| **Java client** | missing `max.in.flight≤5`+`retries>0` (C1); `UNKNOWN_PRODUCER_ID` is abortable not fatal (I1); indeterminate commit window (I3); per-transaction init cost (M1). | C1/I1/I3 **folded** (§4, §5.2, §5.3); M1 → drove **Decision 1B**. |
| **Python/librdkafka** | missing `:retriable` class (C1); group-metadata gap broader than static (C2); reuse model / per-txn cost (I1); acks coerce-when-silent (I2); flush-on-commit barrier (I3). | C1/C2/I2 **folded** (§5.2, §8, §9); I1 → **1B**; I3 (barrier) **folded** as invariant (§4). |
| **Confluent JS / KafkaJS** | raw-pid vs scoped-object safety (C1); shared-static-gap also in KafkaJS (I1); don't-swallow-abort-failure (I2); reuse note (I3); fenced reason atom (M4). | I1/I2/M4 **folded** (§8, §5.4, §5.1); C1 → **open Decision 2**; I3 → 1B. |
| **brod** | brod aborts via trapped `terminate/2` not broker timeout (C1); brod retries all errors blindly — our classifier is *better* (C2); brod doesn't re-find coordinator (C3); brod "documents" gap is false — it only trusts (I1); CTP needs consumer-managed mode + offset bootstrap (I4); auto-id only in processor (I3). | C1 **folded** (§2.4 — Transaction traps exits); C2/C3/I1/I3 → **retuned all "matches brod" to "improves on brod"**; I4 **folded** (§8). |

**Mild pushback recorded:** the JS reviewer's "pid recycling → wrong live process" hazard (R-JS-C1) is largely theoretical on the BEAM; the real value of an opaque handle is the clean `{:error, :transaction_ended}` domain error vs a raw `:noproc` exit. See Decision 2.

---

## 11. OPEN DECISIONS — resolve these first next session

### Decision 1 — begin semantics for the reusable Form 2 (1B)
- **1-A (recommended): explicit `begin_transaction/1`.** Java/librdkafka-faithful; unambiguous `:ready ⇄ :in_transaction` edge; brings back the `:begin` telemetry span; re-justifies the `begin_transaction/1` primitive. Block wrapper/one-shot don't expose it.
- **1-B: implicit begin** (first `produce` after `:ready` starts the next txn). Less ceremony; fuzzy boundary; `commit` with no intervening produce is an awkward no-op.
- *Rec:* **1-A** — we adopted reuse for parity, and all reference clients require explicit begin.

### Decision 2 — Transaction handle type
- **2-A: keep raw pid** (`produce(t, …)` where `t` is the Transaction pid). Simplest; matches the earlier "pid is the dispatch" choice. Calls on a closed/dead `t` surface as `:noproc` exit.
- **2-B: opaque `%KafkaEx.Producer.Transaction.Handle{}`** returning `{:error, :transaction_ended}` on a closed tx. Cleaner error surface; matches KafkaJS's scoped object; costs a wrapper struct + every tx fn takes the handle.
- *Rec (revised under 1B):* lean **2-B**. 1B makes producers **long-lived and reusable**, so "use after `close`/`commit`" is now a realistic mistake (you hold `t` across many transactions). The opaque handle's value rises accordingly. Under pure Y1 (short-lived tx) I'd have said 2-A. **Maintainer to confirm.**

*(Both decisions ripple into §5. Resolve, then update §5.1/§5.3 and §12.)*

---

## 12. PR plan (all merge to `master`; v1.1.0 tag deferred; CHANGELOG under "Unreleased")

- **Cleanup PR (prereq, R1+S1, ~120 LoC):** producer dir reorg, `Partitioner`→`Partitioner.Behaviour`, CHANGELOG. Pure `git mv` + rename.
- **PR 0 — Foundation (~350):** `:required_acks`→`:acks` + deprecation alias; `transaction_coordinators` slot + `NodeSelector.transaction_coordinator/2` + `put/clear_transaction_coordinator`; pure `Transaction.StateMachine` (§5.1) + table tests; **first-class `ErrorClassifier` with the §5.2 table** (not a skeleton) + tests; CHANGELOG.
- **PR 1 — Idempotent producer (~800):** `Client.IdempotentState` (ETS `:protected`, atomic counter, monitor-based cleanup, full docs); `KafkaEx.Producer.Request` (§4) + byte-identical-retry integration test; `enable_idempotence: true`; `InitProducerId(transactional_id: nil)` routing; RecordBatch header-field population + `attributes_for_record_batch/2`; Z1 acks check (explicit-conflict-only) + `retries > 0` check; `max.in.flight` invariant doc + regression note; telemetry `:pid_assigned`, `:sequence_rejected`; concurrency test.
- **PR 2 — Transactional producer (~1200):** `KafkaEx.Producer.Transaction` gen_server (1B reusable lifecycle, traps exits, linked); `init_transaction/2` (validated id, settable `transaction_timeout_ms`); `begin_transaction/1` (if Decision 1-A); `commit_transaction/1` (→ `:ready`), `abort_transaction/1`, `close_transaction/1`; full §7 choreography (lazy `AddPartitionsToTxn`, dual-coordinator routing, `EndTxn` boolean); coordinator failover in Client; `KafkaEx.API.transaction/3` block wrapper + `rollback/2` + abort-failure wrapping; `use KafkaEx.API, transactional_id:` (AA1, id-string only); handle type per Decision 2; exit-matrix test (§5.4) incl. indeterminate-commit test; telemetry `:init`/`:begin`(if 1-A)/`:add_partitions`/`:commit`/`:abort`/`:fenced`/`:state_transition`; docs.
- **PR 3 — Consume-process-produce (~600):** `add_offsets/3` (dynamic only; §7 steps 4–5; honesty caveat in docstring/CHANGELOG); GenConsumer `{:no_commit, state}` + **consumer-managed mode** + offset-bootstrap path; `set_offset/2`, `force_commit/1`; async-commit timer reset; end-to-end CTP integration test; telemetry `:add_offsets`; docs.

---

## 13. Decision ledger
| ID | Decision | Status |
|---|---|---|
| A2 | Shared Client ETS `{producer_id, topic, partition}` for all sequences | locked |
| C1 | `Client.transactional_produce/N` owns lookup/build/dispatch | locked |
| E2′ | Cleanup via Client `monitor` `:DOWN` + graceful `terminate/2`; only on terminate/epoch-bump | locked (rev. by review) |
| F2 | idempotence flag ⟂ transactions | locked |
| G2 | 3-layer retry stability (struct + single site + integration test) | locked |
| R1 | producer dir cleanup = prerequisite PR | locked |
| S1 | `Partitioner` → `Partitioner.Behaviour`, accept break | locked |
| T2′ | collapse `:committing`/`:aborting`/`:transaction_busy`; keep `:ready` (1B) | locked (rev. by 1B) |
| U1 | wrapper no trap; **Transaction gen_server traps** to abort in terminate | locked (rev. by review) |
| Y1→1B | reusable manual lifecycle; block/one-shot terminate on commit | locked |
| AA1 | `use KafkaEx.API, transactional_id:`; caches id string only | locked |
| Z1′ | reject acks only on explicit conflict; unset→-1; no raise | locked (rev. by review) |
| AB1 | ship `add_offsets/3` on V2; document the (broader) gap | locked |
| O1 | `built_at_ref` identity-only, no tripwire | resolved |
| O2 | drop 249-byte `transactional_id` check | resolved |
| **D1** | **begin semantics (explicit vs implicit)** | **OPEN — rec 1-A** |
| **D2** | **handle type (raw pid vs opaque handle)** | **OPEN — rec 2-B under 1B** |

---

## Appendix — key code references (verified this pass)
- Retry loop / single build: `lib/kafka_ex/client/client.ex:592` (`do_produce_request`), `:799-855` (`handle_request_with_retry`), `:581` (`:required_acks` telemetry).
- Produce builder (RecordBatch attrs/header target): `lib/kafka_ex/protocol/kayrock/produce/request_helpers.ex:32,112-135`.
- Request construction site: `lib/kafka_ex/client/request_builder.ex:214-225`.
- Coordinator cache + failover precedent: `lib/kafka_ex/cluster/cluster_metadata.ex` (has `put_consumer_group_coordinator/3`, no `clear_*`); `client.ex:1056` (`update_consumer_group_coordinator`).
- Consumer hooks: `lib/kafka_ex/consumer/gen_consumer.ex:805-832` (best-effort `terminate/2` precedent), `:866-902` (handler dispatch), `:934-960` (async-commit timer).
- `use KafkaEx.API` macro: `lib/kafka_ex/api.ex:97`; produce + nil-partition path: `:717-790`.
- FindCoordinator type mapping: `lib/kafka_ex/protocol/kayrock/find_coordinator/request_helpers.ex:55-62`.
- Kayrock wire versions: `/Users/piotr.r/Projects/kayrock/lib/generated/{init_producer_id,add_partitions_to_txn,add_offsets_to_txn,end_txn,txn_offset_commit,find_coordinator}.ex`; RecordBatch: `/Users/piotr.r/Projects/kayrock/lib/kayrock/record_batch.ex:46-59,135-138`.
