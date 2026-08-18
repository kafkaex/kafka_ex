# RFC 0001: Non-blocking process architecture for `KafkaEx.Client`

- **Status:** Draft — request for comments
- **Author:** (KafkaEx maintainers / proposer)
- **Date:** 2026-07-28 (last updated 2026-08-18)
- **Target release:** v1.2.0 (delivered incrementally; the public API's shape is unchanged throughout)
- **Related issues:** #357 (`KafkaEx.stream` times out fetching at log-end), #445 (metadata cache not updated on produce)
- **Supersedes** four earlier internal design notes, whose substance is carried in full by this
  document (they are not repository artifacts and this RFC does not depend on them): evicting the
  socket from `Broker` and naming the connection; decentralising the data plane into per-broker
  connections; deepening `ClusterMetadata`; and a cluster-shared metadata store — the last of which is
  superseded rather than adopted, in favour of per-client infrastructure (see Alternatives).

## Contents

- [Summary](#summary) · [Non-goals](#non-goals)
- [Current Limitations](#current-limitations) — the three drivers, in code
- [Current Architecture](#current-architecture) · [End State Architecture](#end-state-architecture)
  ([Sequence](#sequence), [Supervision tree](#supervision-tree), [Startup](#startup),
  [State machines](#state-machines))
- [Design decisions](#design-decisions-resolved-review) — the 20-row table reviewers should read first
- [Delivery](#delivery) — the 7-PR plan, [Rollback](#rollback), [Test strategy](#test-strategy),
  [Benchmarks](#benchmarks), [Success criteria](#success-criteria)
- [Drawbacks](#drawbacks) · [Rationale and alternatives](#rationale-and-alternatives) ·
  [Security considerations](#security-considerations) · [Prior art](#prior-art)
- [Unresolved questions](#unresolved-questions) ·
  [Forward compatibility](#forward-compatibility--does-this-foreclose-anything) ·
  [Future possibilities](#future-possibilities) · [References](#references)

## Summary

Split the single `KafkaEx.Client` `GenServer` into a small process tree that separates the
**control plane** (metadata, coordinator resolution, routing, retry) from the **data plane**
(per-broker socket I/O), so that no single slow or unresponsive broker can block unrelated work.
Each client owns its **own** supervised infrastructure — one `MetadataStore` and one set of
`Connection`s per client — independently supervised from the front so that blocking socket I/O and
metadata refresh never share the front's mailbox. Infrastructure is deliberately **per-client, not
shared across clients**: this mirrors the Java client and librdkafka (per-instance) and brod (which
shares only through an explicitly named client). The "N clients per consumer group" footprint is
addressed by the existing opt-in shared `:client` (`GenConsumer.resolve_client`, #581), **not** by
auto-keyed cross-client sharing, which is left as a future option. The change is **fully backward
compatible**: the public API, the `client` process identity and message contract, telemetry event
shapes, and error atoms are unchanged. One behavioural change is deliberate and stated in the
compatibility contract below: request order is preserved **per caller process** rather than globally
across all callers, as today's single mailbox incidentally does.

## Non-goals

Stating these up front, because each has been proposed before and each would change what a reviewer
is being asked to approve:

- **No public API change.** Not a single new function, option or return shape is required by this
  work. Anything additive (an async request primitive, pause/resume, a rebalance listener) is a
  separate proposal — see Forward compatibility.
- **No process-per-topic-partition data plane.** We adopt the per-broker (× role) split and stop
  there; the brod-style producer/consumer process per partition is explicitly rejected in
  Alternatives.
- **No cross-client sharing of metadata or connections.** Infrastructure is per-client. The
  N-clients-per-group footprint is *not* addressed here and stays collapsible the way it already is,
  via a shared `:client`.
- **No incremental fetch sessions (KIP-227), no idempotent or transactional producer, no cooperative
  rebalancing.** The design must not foreclose them, which is a weaker and cheaper obligation than
  building them.
- **No performance tuning as a goal in itself.** The target is removing head-of-line blocking. A
  throughput gain at high concurrency is expected to follow, but the benchmarks exist to catch a
  *regression*, not to chase a number.

## Current Limitations

`KafkaEx.Client` today is one `GenServer` that fuses three responsibilities into a single mailbox:

1. **Blocking per-broker socket I/O.** `NetworkClient.send_sync_request/3` flips the socket to
   `{active, false}` and blocks on `Socket.recv/3` **inside `handle_call`**
   (`lib/kafka_ex/network/network_client.ex:114-158`, driven from `client.ex network_request/4`).
2. **Metadata ownership and refresh.** Broker sockets live in `Broker.socket` inside
   `cluster_metadata` inside `Client.State`; the periodic refresh runs
   `:timer.send_interval → handle_info(:update_metadata)` in the *same* mailbox
   (`client.ex:171,384`).
3. **Retry / routing orchestration** (`handle_request_with_retry`, `refresh_for_error`).

A single monotonic `correlation_id` in `State` (`client.ex:1370`) means the client holds **at most
one request in flight** at any moment. Because everything shares one mailbox and one blocking
`recv`, a single broker freezes the whole client:

- **#357** — a long-poll `fetch` at the log end holds the `GenServer` for its full `wait_time`
  (up to ~60 s), so every other caller `GenServer.call` times out.
- **#445** — metadata reads and refresh queue behind slow socket I/O, so a produce can observe stale
  or mismatched topic metadata.
- **The black-holed-broker stall** (an internal finding; stated in full here because it is not a
  repository artifact) — one broker that is "black-holed" (TCP up, no application reply)
  monopolises the shared client; the consumer-group `Heartbeat`'s queued call then exits `:timeout`,
  the `Manager` rebalances, and if the stall exceeds `session_timeout` the coordinator evicts the
  member: **a group-wide rebalance triggered by one unrelated broker**.
- The v1.1.0 timeout changes (`request_timeout` 1 s → 15 s, `heartbeat_interval` 5 s → 3 s, fetch
  transport `:timeout` retried ×3) **lengthened these blocking windows** on top of the shared
  mailbox.

Separately, because each client owns its own metadata and sockets, a consumer group holds **N copies**:
in production the `Manager` **always** starts its own `Client` (its `:client` opt is test-only,
`manager.ex:266-270`), and every `GenConsumer` starts its own unless handed a shared `:client`
(`gen_consumer.ex:641,679-693`; `resolve_client/3`, #581) — so N metadata catalogs, N refresh loops,
and N socket sets to the same brokers.

Today's request flow — one blocking `recv` in one mailbox means one broker freezes everything:

```mermaid
sequenceDiagram
    participant S as Stream (long-poll fetch)
    participant HB as Heartbeat
    participant C as KafkaEx.Client (single GenServer)
    participant BrA as Broker A
    S->>C: GenServer.call {:fetch} (wait_time up to 60s)
    activate C
    C->>BrA: send + blocking recv
    Note over C: single mailbox blocked for up to 60s
    HB->>C: GenServer.call {:heartbeat} (waits in mailbox)
    Note over HB,C: heartbeat cannot be served -> exits :timeout -> rebalance
    BrA-->>C: response (or :timeout)
    deactivate C
    C-->>S: reply
```

The goal is to remove head-of-line blocking and make metadata handling correct **without changing
any public API** — the client must stay a drop-in for every existing caller (`KafkaEx.API`, the
legacy `KafkaEx.*` worker API, `ConsumerGroup.Manager`, `Heartbeat`, `GenConsumer`, `Stream`).

**Expected outcome (by design):** #357 and #445 eliminated by the split, the black-holed-broker stall
removed architecturally, and metadata refresh, heartbeats and produce to independent brokers
proceeding in parallel — with zero changes required from any library consumer. The
N-clients-per-group footprint is **not** removed here; it remains collapsible, as today, by handing
the group a shared `:client` (`resolve_client`, #581). Cross-client sharing of metadata and connections is a
possible future option (see Alternatives), not part of this change.

## Current Architecture

Where this change sits in the existing module layout, in the project's own vocabulary — *public
surface → client → dispatch hub → per-operation protocol modules*, plus *control plane / data plane*,
*node selector*, *coordinator*, *cluster metadata model*, and *native message structs*.

```mermaid
flowchart TD
    subgraph callers["Callers"]
        CG["Consumer.* — GenConsumer, ConsumerGroup{Manager, Heartbeat}, Stream"]
        PROD["Producer.* — partitioner + produce path (+ Legacy)"]
        USER["user code"]
    end
    USER --> API
    CG --> API
    PROD --> API
    API["KafkaEx.API — public surface (+ legacy KafkaEx.* worker API)"]
    API -->|"GenServer.call(client, {op,...})"| CLIENT
    CLIENT["KafkaEx.Client — GenServer<br/>today: control plane + data plane in one mailbox"]
    CLIENT --> BUILD["RequestBuilder → KayrockProtocol (dispatch hub)"]
    BUILD --> IMPL["per-operation protocol modules<br/>Request/Response protocols + vN impls"]
    CLIENT --> PARSE["ResponseParser ← KayrockProtocol"]
    CLIENT -. "owns (State)" .-> STATE["Client.State + Cluster.ClusterMetadata<br/>(Broker / Topic / TopicPartition / PartitionInfo)"]
    CLIENT --> NET["Network.NetworkClient / Socket<br/>blocking send + recv"]
    NET --> KAFKA(("Kafka brokers"))
    IMPL --> MSG["Messages.* — native structs returned to caller"]
```

**Layers (top → bottom):**

1. **Public surface — `KafkaEx.API`** (`api.ex`, `api/behaviour.ex`). The primary interface; every
   function takes `client` first and issues `GenServer.call(client, {op, …}, budget)`, returning
   `{:ok, result}` / `{:error, reason}` where `result` is a `Messages.*` struct. The legacy worker
   API (`kafka_ex.ex`: `create_worker`, `start_link_worker`, `KafkaEx.Supervisor`) sits alongside it
   for backward compatibility.
2. **Client — `KafkaEx.Client`** (`client/client.ex`), the `GenServer` that owns a cluster
   connection. Today it fuses **control plane** and **data plane**. Its support cast in `client/`:
   `state.ex` (`Client.State`), `node_selector.ex` (`NodeSelector`: `:first_available` /
   `:topic_partition` / `:consumer_group`), `request_builder.ex` / `response_parser.ex`,
   `request_context.ex` (`RequestContext`), `request_budget.ex`, `metadata_log.ex` (edge-triggered
   missing-topic logging), `error.ex`; plus shared `Support.Retry`, `Support.OptionalDeps`,
   `Telemetry` (the `[:kafka_ex, :request]` span), and `Config`.
3. **Dispatch hub — `Protocol.KayrockProtocol`** (`protocol/kayrock_protocol.ex`). The only place the
   rest of the client talks to the protocol layer: `build_request(op, api_version, opts)` and
   `parse_response(op, response)` branch by `{operation, version}`. Slated to become a separate
   package post-1.0.
4. **Per-operation protocol modules** (`protocol/kayrock/<operation>/`). Each operation defines the
   `Request`/`Response` Elixir protocols (`@fallback_to_any true`) plus one
   `vN_request_impl.ex` / `vN_response_impl.ex` per API version, an `any_*_impl.ex` forward-compat
   fallback, and shared `request_helpers.ex` / `response_helpers.ex`.
5. **Network — `Network.NetworkClient` / `Network.Socket`** (`network/`). Stateless plumbing:
   `create_socket`, `send_sync_request` (`{:packet, 4}` framing, the **blocking `Socket.recv`** that
   this RFC targets), `close_socket`. `network/behaviour.ex` is the mock seam (Mimic).
6. **Data model — `Cluster.*` and `Messages.*`.** `Cluster.*` (`cluster_metadata.ex`, `broker.ex`,
   `topic.ex`, `topic_partition.ex`, `partition_info.ex`) is the broker/topic/partition/leader model;
   `ClusterMetadata` also carries the **coordinator cache** and today transplants live sockets in
   `Broker.socket`. `Messages.*` are the native structs returned to callers (`Fetch`, `Fetch.Record`,
   `RecordMetadata`, `Offset`, `ConsumerGroupDescription`, …).

**Callers of the client.** All reach the client through `KafkaEx.API` → `GenServer.call(client, …)`:
`ConsumerGroup.Manager` (join / sync / leave, offset fetch / commit), `ConsumerGroup.Heartbeat`
(**its call is the one that exits `:timeout`** when the client is blocked → the black-holed-broker stall), `GenConsumer`
(fetch, offset commit), `Stream` (fetch; the long-poll at log-end is **#357**), `Producer.*`, and the
legacy `KafkaEx.*` worker API.

**Consumer-group process tree, and where the client comes from:**

```mermaid
flowchart TD
    CG["ConsumerGroup (Supervisor, one_for_one)"]
    CG --> MGR["Manager (GenServer)"]
    CG --> GCS["GenConsumer.Supervisor (DynamicSupervisor)"]
    MGR -->|"start_link"| HB["Heartbeat (GenServer)"]
    GCS --> GC1["GenConsumer #1 (partition 0)"]
    GCS --> GC2["GenConsumer #N (partition k)"]
    MGR -. "Client.start_link(:no_name) if no shared :client" .-> CL1["Client (own)"]
    GC1 -. "resolve_client: shared :client or own Client" .-> CL2["Client (own / shared)"]
```

**Key fact (verified in code):** in production the `Manager` **always** starts its own `Client` (its
`:client` opt is test-only), and every `GenConsumer` starts its own unless handed a shared `:client`.
Hence the N-clients-per-group footprint named above — one this RFC leaves collapsible for the
`GenConsumer`s via a shared `:client`, not via auto-sharing (the `Manager`'s own client stays
separate). The new processes this RFC introduces (`Connection`, `ConnectionSupervisor`,
`MetadataStore`, and the supervisor that groups them) are **per-client infrastructure**: each
`Client` keeps its identity as a thin front and owns its own store and connection set, independently
supervised from the front.

## End State Architecture

The mental model is a **control plane vs data plane** split, on two axes:

- **Data plane** = per-broker, per-role `Connection` processes. Each owns exactly one socket and its
  own `correlation_id`, is **event-driven** (`{active, once}` — never a blocking `recv`), and blocks
  only itself. A slow fetch on broker A no longer affects broker B, and coordinator/heartbeat traffic
  rides a *different* connection than data traffic even to the same broker. Connections belong to
  **one client**, resolved through that client's own `Registry`.
- **Control plane** = routing + retry (the front `KafkaEx.Client`, one per user client) and metadata +
  coordinator discovery (a **`MetadataStore` owned by that same client**). Each client owns the
  metadata for its own connection to the cluster.

Infrastructure is **per-client**: each `KafkaEx.Client` owns its own `MetadataStore` and connection
set, and two clients — even with identical credentials to the same cluster — never share a socket or
a metadata refresh. This keeps the trust boundary trivially safe (nothing crosses between clients)
and is the same choice the reference clients make by default (Java/librdkafka per-instance; brod
shares only via an explicitly named client). Auto-sharing metadata/connections across clients that
happen to share a `{bootstrap, ssl, auth}` identity is a **future option** (see Alternatives), not
built here.

From a caller's perspective **nothing changes**. A caller still writes:

```elixir
GenServer.call(client, {:fetch, topic, partition, offset, opts}, budget)
```

Under the hood the front `KafkaEx.Client` no longer blocks: it looks up the target (a lock-free ETS
read from the `MetadataStore`), hands the request to that broker/role `Connection` asynchronously,
and parks the caller until the answer comes back — while remaining free to serve heartbeats, metadata
refreshes, and requests to other brokers.

**Backward-compatibility contract (invariant):**

- `KafkaEx.Client` remains the named front process answering the same `GenServer.call({op, …})`
  messages.
- `start_link(args, name)` still returns `{:ok, pid}`, registers `name`, and is ready on return
  (synchronous fail-fast boot).
- `send_request/4`, `retry_count/0`, `coordinator_max_attempts/0` are unchanged. `send_request/4` in
  particular keeps **send-once, no-retry** semantics: today its handler bypasses the retry loop
  entirely (`client.ex:371-375`, unlike `handle_request_with_retry`), so the front must **exempt** it
  from the uniform `{:done, …}` retry classification. Without that exemption it would silently gain
  retries — one `:timeout` today, up to three afterwards. It remains a deliberate escape hatch to the
  ~28 Kafka APIs Kayrock generates but `KafkaEx.API` does not wrap, and is documented as such in
  `usage-rules.md`.
- **Error atoms are unchanged, and this RFC introduces none.** Every failure mode maps onto an atom
  already in `KafkaEx.Support.Retry`'s classification, because an unknown atom falls through
  `transient_error?/1` to `false` and would silently turn a retryable condition into a fatal one. The
  atoms the front returns are exactly today's: `:timeout` (written but unanswered — retryable, except
  on coordinator requests where send-once applies), `:not_connected` (never written — parks, never
  fatal), `:no_broker` (no target selectable, including from a `:degraded` store), plus the unchanged
  broker error codes.
- **One behavioural change, stated deliberately.** Today's single `GenServer` imposes a *global total
  order* on every request from every caller — an accident of having one mailbox. The front preserves
  order only **per caller process**; requests from different processes become genuinely concurrent and
  may reach the broker in either order. Worth naming concretely: two processes committing offsets for
  the same partition can now move the committed offset backwards (redelivery — at-least-once still
  holds, nothing is lost). The common path is unaffected, because `GenConsumer` and `Stream` block on
  their own commit (`gen_consumer.ex:1026`, `stream.ex:182`), so at most one commit per
  (group, topic, partition) is ever in flight. **That is a load-bearing invariant of the consumer
  layer, not a coincidence** — any future change that stops the consumer blocking on its commit must
  supply an equivalent guarantee. We deliberately do *not* add a commit-ordering gate: none of the
  three reference clients gates commits (Java's consumer is single-threaded per instance; brod and
  librdkafka serialize through the group coordinator).
- All telemetry event **shapes** (names + measurements) are unchanged. Operation spans
  (`:produce`/`:fetch`/`:consumer.*`) stay in the front (same pid as today's client); the per-attempt
  `[:kafka_ex, :request]` span and the `:connection`/`:auth`/`:connection.close` events move to the
  `Connection` (their emitting pid changes). Under the async model emission shifts from the synchronous
  `Telemetry.span/3` wrapper to manual `:start`/`:stop`.
- The front client stays the failure unit that `ConsumerGroup` supervision monitors.
- `MetadataStore`, `Connection`, the per-client `Infra.Supervisor` grouping them, and the connection
  `Registry` are strictly internal; callers never address them.
- The OTP application boot (`KafkaEx.Supervisor`) and the `disable_default_worker` flag are unchanged:
  a disabled default worker still means no client and no connections until one is explicitly started,
  so `mix test.unit` still needs no Kafka cluster.

### Sequence

The caller keeps issuing a blocking `GenServer.call`. The front is a plain `GenServer` that
multiplexes many in-flight requests; each request's state lives in a `pending` map keyed by `ref`,
**not** in the process state (so `:gen_statem` does not fit the front — see Design decisions).

**What a `pending` entry holds.** Besides the `from` and the request: the current phase, the
persistent `attempt` counter, a **monitor on the caller** (whose pid arrives free in `from`), the
per-partition **gate slot** it holds (produce only), and an **absolute deadline**. The deadline is
admitted *with* the request. `KafkaEx.API` already computes the caller's budget via
`RequestBudget.call_budget/2` (`api.ex:967`) but passes it **only** as the `GenServer.call` timeout,
so the server never sees it — which means a promise like "no caller parked past its own deadline" is
not implementable until that number crosses the process boundary. It is therefore threaded into the
request message and stored as `deadline = monotonic_now + budget`. Re-deriving it inside the front
from `network_timeout` and `@retry_count` is the tempting shortcut and is wrong: `send_request/4`
lets a caller pass an arbitrary explicit timeout (`client.ex:1294`), and two independent computations
of one budget is exactly the drift #562 closed by making `@retry_count` the single source of truth.

Phases:

- `:resolving` — pick the target. Read the leader/coordinator from the `MetadataStore`'s ETS
  (lock-free). Hit → dispatch to the right `(node, role)` `Connection` (→ `:in_flight`). Miss → cast
  "refresh X" to the store and park as a continuation (→ `:awaiting_metadata` / `:awaiting_coordinator`).
- `:in_flight` — sent to a `Connection`; awaiting `{:done, ref, result}`.
- `:awaiting_metadata` / `:awaiting_coordinator` — waiting for the store's (coalesced) refresh to
  complete and notify; on notification, re-enter `:resolving`.
- `:awaiting_connection` — the target `Connection` refused a request it never wrote
  (`{:error, :not_connected}`); the entry waits for that connection to report `:connected`, bounded by
  its own deadline. This costs **no** retry attempt — see `Connection`, below, for why.

On `{:done, ref, result}` the front runs the **existing** retry classification
(`Retry.leadership_error?` / `coordinator_refresh_error?` / transport-timeout):

- success → `GenServer.reply(from, result)`, terminal;
- retryable leadership error → cast "refresh this topic/partition" to the store (coalesced per key in
  its `in_flight` map), → `:awaiting_metadata`, decrement budget;
- retryable coordinator error → cast "re-discover" to the store, → `:awaiting_coordinator`;
- transport timeout **on a coordinator request** → reply error immediately (send-once). Today's
  `@coordinator_max_attempts = 1` stays a **structural** cap on JoinGroup/SyncGroup — not an
  error-keyed one — so it protects them whatever the transport returns; rejoin/commit re-issue higher
  up;
- other transport timeout (data) → re-dispatch (→ `:in_flight`), decrement budget;
- `{:error, :not_connected}` → `:awaiting_connection`, budget untouched;
- non-retryable / budget exhausted → reply error, terminal.

**The terminal paths are the load-bearing part.** An entry that ends without being cleaned up does not
merely leak memory: while it holds a produce-gate slot at `N = 1` it stalls its entire partition,
silently and indefinitely — the very failure class (#357) this RFC exists to remove. An entry is
dropped, its caller demonitored and its gate slot released (waking the next FIFO waiter) on **all** of:
reply to the caller, retry-budget exhaustion, **deadline expiry**, **caller `:DOWN`**, and
`Connection` `:DOWN`. The deadline and the caller monitor are complementary, not redundant — the
monitor catches a caller that died (which is what a `GenServer.call` timeout does to an ordinary
process), the deadline catches one that survived by catching the exit, and the deadline additionally
stops the front from *starting* an attempt that cannot finish in time.

**On backpressure.** The blocking contract does bound concurrency — one in-flight call per caller
process — but it bounds the `pending` map only for *live* callers. Boundedness under caller churn
comes from the monitors and deadlines above, not from the blocking contract itself. In-flight requests
per connection are therefore ceilinged by the number of concurrently calling processes; we state that
as an invariant rather than leave it an accident, and defer an explicit per-connection cap to the
future async primitive that breaks the one-call-per-caller property (see Broadway / GenStage). Java's
`max.in.flight.requests.per.connection` (default 5) exists mainly to bound reordering under retry —
which decision #13's `N = 1` produce gate already covers here — so a second cap would buy no
correctness we do not already have.

```mermaid
sequenceDiagram
    participant Caller
    participant Front as KafkaEx.Client front
    participant Conn as Connection broker+role
    Caller->>Front: GenServer.call op (blocking)
    Front->>Front: read target from MetadataStore ETS - park caller under ref
    Front->>Conn: send {:run ref wire}
    Note over Front: returns :noreply - free for other work
    Conn->>Conn: send + event-driven recv - blocks only itself
    Conn->>Front: {:done ref result}
    Front->>Front: retry / trigger store refresh if needed
    Front-->>Caller: GenServer.reply(from result)
```

The one substantive refactor is turning the recursive retry loop — whose state currently lives on a
blocking stack frame — into this `ref`-keyed state machine. Retry stays centralized in the front;
metadata refresh is delegated to the store; only blocking I/O moves out. Per-attempt timeout lives in
the `Connection` (it holds the send timestamp) and is reported to the front as
`{:done, ref, {:error, :timeout}}`; the **retry decision** stays in the front. A `Connection` `:DOWN`
fails every `pending` entry routed to it — one of the five terminal paths above, not the only one.
Replying to a `from` whose caller already timed out is a harmless no-op.

Each physical dispatch uses a **fresh `ref`** as its `pending` key, so a late `{:done, …}` from a
superseded or timed-out attempt lands on no entry and is dropped; the connection boundary reinforces
this — a re-dispatch after a leadership change goes to a *different* `Connection` process (its own
`correlation_id` space), and a dead one's entries are already failed via `:DOWN`. Distinct from that
matching key, a **separate `attempt` counter** rides the logical request (it *survives* every retry,
whereas the key dies with each attempt) and drives backoff, the retry budget, and the telemetry
attempt number. The two are deliberately **not** one token: their lifetimes are opposite.

### Supervision tree

Edge styles follow the legend below the diagram: thick `⇒` = link / supervision (lifetime-coupled),
dotted `⇢` = monitor or lock-free use (no lifetime coupling).

```mermaid
flowchart TD
    PARENT["parent (links to the front)<br/>KafkaEx.Supervisor · or ConsumerGroup Manager / GenConsumer"]
    PARENT ==>|link| FRONT["KafkaEx.Client — front<br/>GenServer · does NOT trap exits · the caller-facing pid"]
    FRONT ==>|"link — starts it in init"| ISUP["Infra.Supervisor<br/>:rest_for_one"]
    ISUP ==>|supervises| REG["Registry<br/>{host, port, role}"]
    ISUP ==>|supervises| STORE["MetadataStore<br/>:gen_statem · :transient · owns ETS (single-writer)"]
    ISUP ==>|supervises| CONNSUP["ConnectionSupervisor<br/>DynamicSupervisor"]
    CONNSUP ==>|"supervises · :temporary"| CD["Connection :data<br/>:gen_statem · traps exits · closes socket in terminate · bounded shutdown"]
    CONNSUP ==>|supervises| CC["Connection :coordinator"]
    CONNSUP ==>|supervises| CM["Connection :metadata"]
    FRONT -. "read ETS (lock-free)" .-> STORE
    FRONT -. "route via" .-> REG
    FRONT -. "monitor (per in-flight dispatch)" .-> CD
    STORE -. "monitor + refresh via" .-> CM
    CD --> BK(("leader broker"))
    CC --> BC(("group coordinator"))
```

**Edge legend.** Thick `⇒` = **link / supervision** (lifetime-coupled: if the source dies, the
target is torn down). Dotted `⇢` = **monitor or lock-free use** (a one-way death signal, or an
ETS/Registry read — *no* lifetime coupling). A second client is an identical, fully independent
subtree hanging off its own parent; nothing is shared between clients.

**Lifetime & teardown (resolved).** The front is the process the parent links to and callers hold;
in its `init` it starts (and thereby links to) its own `Infra.Supervisor`. Teardown rides that link,
**not `terminate/2`**: when the front stops — a `GenServer.stop` from a `Manager`/`GenConsumer`, or a
`:shutdown` from the application supervisor — its supervised `Infra.Supervisor` is torn down in order,
and each `Connection` closes its socket in its **own** `terminate/2` (so a non-trapping front skipping
`terminate/2` on `:shutdown` costs nothing). The front does **not** trap exits: a crash of an
individual `MetadataStore`/`Connection` is absorbed by the `Infra.Supervisor`'s restart and the front
only re-resolves off the monitor `:DOWN`; the front dies only if the `Infra.Supervisor` itself
exhausts its restart intensity, at which point its parent recreates the whole client. `Connection`s
carry a bounded shutdown so that N per-partition `:ssl.close/1` calls (closed in parallel under the
`DynamicSupervisor`) cannot exceed the caller's teardown budget.

**A concrete instance — three brokers.** The diagram above is schematic (it shows *roles*, not
counts). Instantiated for a 3-broker cluster (`b1`/`b2`/`b3`) with one topic `orders` whose partitions
`p0`/`p1`/`p2` lead on `b1`/`b2`/`b3`, a consumer group whose coordinator is `b2`, and a client
consuming all three partitions:

```mermaid
flowchart TD
    FRONT["KafkaEx.Client — front  (1 per client)"]
    FRONT ==>|link| ISUP["Infra.Supervisor  (1)"]
    ISUP ==> REG["Registry  (1)"]
    ISUP ==> STORE["MetadataStore  (1)"]
    ISUP ==> CONNSUP["ConnectionSupervisor  (1)"]
    CONNSUP ==> M["Conn {b1, :metadata}"]
    CONNSUP ==> CO["Conn {b2, :coordinator}"]
    CONNSUP ==> D0["Conn {b1, :data} — fetch p0"]
    CONNSUP ==> D1["Conn {b2, :data} — fetch p1"]
    CONNSUP ==> D2["Conn {b3, :data} — fetch p2"]
    STORE -. monitor .-> M
    FRONT -. monitor .-> CO
    FRONT -. monitor .-> D0
    FRONT -. monitor .-> D1
    FRONT -. monitor .-> D2
    M --> B1(("broker b1"))
    D0 --> B1
    CO --> B2(("broker b2"))
    D1 --> B2
    D2 --> B3(("broker b3"))
```

The singletons (`front`, `Infra.Supervisor`, `Registry`, `MetadataStore`, `ConnectionSupervisor`) are
**one each regardless of broker count**; only `Connection`s scale, one per `{broker, role}` in use.
One physical broker can back **several** sockets under different roles — here `b1` carries both
`:metadata` and the `p0` `:data` socket, `b2` both `:coordinator` and the `p1` `:data` socket — which
is exactly the role separation that removes the Heartbeat-vs-fetch coupling behind the
black-holed-broker stall. The number of
`:data` `Connection`s is the one variable the fetch model sets: **per-partition** (default) gives one
per consumed partition (two partitions sharing a leader ⇒ two sockets to that broker), while the
`share_leader_conn` knob collapses them to one per broker.

An earlier draft justified that knob by a KIP-227 fetch-session-slot ceiling. **That justification does
not hold and has been removed:** KafkaEx sends `session_id: 0` with `epoch: -1` on every v7+ fetch
(`protocol/kayrock/fetch/request_helpers.ex:121-122`), nothing in `lib/` ever overrides either, and the
`session_id` a broker returns is never threaded into the next request. KafkaEx therefore never sustains
an incremental fetch session — every fetch is a full fetch — so per-partition connections multiply
nothing on the broker's session cache. The ceiling becomes real only if incremental fetch is adopted
later, at which point it is a **precondition of that change**, not a cost of this one. The knob's
justification is the ordinary one: fewer sockets and fewer broker-side connections when a consumer
holds many partitions on one broker.

**Who owns what.**

- **`KafkaEx.Client` (front).** A plain `GenServer`, one per user client — it *must* be the process
  returned by `start_link/2` and registered under `name`, because callers hold that pid and issue
  `GenServer.call` on it, so it cannot itself be a supervisor. It routes, retries, and holds parked
  callers in the `pending` map; it never does blocking I/O. Its infrastructure (`MetadataStore`,
  `Registry`, `ConnectionSupervisor`) is owned by this client via an `Infra.Supervisor` it **starts
  and links to in `init`** (the front does not trap exits); that link makes teardown automatic (see
  Lifetime & teardown) and keeps the front the caller-facing pid without itself being a supervisor.
- **`Infra.Supervisor` (per client, `:rest_for_one`).** Groups, in order, the client's `Registry`,
  `ConnectionSupervisor`, then `MetadataStore` — Registry first so the others can register, and
  `ConnectionSupervisor` before the store because the store obtains its `:metadata` `Connection`
  *through* the `ConnectionSupervisor` (so under `:rest_for_one` a `ConnectionSupervisor` restart also
  restarts the store, its dependant). Started and linked by the front in `init`. A `MetadataStore`/`Connection` crash is absorbed by a restart
  here and never takes the front down; the whole subtree stops with the client because the front is
  its supervised parent (the link). `MetadataStore` is `:transient` and the supervisor's restart
  intensity is tuned so transient store flapping self-heals rather than toppling the front.
- **`MetadataStore`.** A `:gen_statem` (states `:loading`/`:ready`/`:degraded`; fail-fast `init`).
  Owns the metadata ETS table (lock-free reads), is the single writer and the per-key
  refresh/coordinator-discovery coalescer, and uses a `:metadata`-role connection to refresh
  (mirroring brod's dedicated `meta_conn`). Reads never touch its mailbox.
- **`ConnectionSupervisor` + `Connection`s.** Per-broker, per-role `:gen_statem` connections
  (see State machines), `:temporary` children created lazily and registered in the `Registry` keyed by
  `{host, port, role}` (ssl/auth are implied by the owning client's identity).

**Failure blast radius.**

- **A `Connection` crash is isolated.** Its front fails the `pending` entries routed to it (never
  leaked) and it is recreated lazily on the next request — no eager reconnect storm.
- **A front crash affects only its own callers** — and, because infrastructure is per-client, its own
  infrastructure. No other client is touched (there is nothing shared to touch).
- **A `MetadataStore` crash** restarts under its client's own supervisor. Its ETS table dies with it
  (a dead owner's tables are destroyed on the BEAM), so the restarted store re-enters `:loading` and
  rebuilds the snapshot; during that brief gap the front's reads miss → it parks callers in
  `:awaiting_metadata` and re-resolves once the store is `:ready` again. The gap is one metadata fetch
  and touches only this client.
- **`MetadataStore` (single writer) closes the #445 concurrency class** for its client: concurrent
  produces to a missing topic coalesce onto one refresh instead of racing.

**Links, monitors, and who cleans up.** The design uses three distinct BEAM mechanisms on purpose —
supervision **links** (shared lifetime), **monitors** (a one-way death signal that does *not* couple
lifetime), and **`Registry`** (which monitors its registrants for us). There is **no hand-rolled
"live connections" table** that could drift from reality: a dead process simply disappears from the
registry. Reading the edges as *observer → observed*:

| Observer → Observed | Mechanism | What happens when the observed dies |
|---|---|---|
| `ConnectionSupervisor` → `Connection` | supervision link, `:temporary`, bounded shutdown | **not** restarted (temporary); recreated lazily on the next request. On an ordered shutdown the `Connection` (which traps exits) closes its socket in its own `terminate/2` |
| connection `Registry` → `Connection` | Registry's built-in monitor | Registry **auto-removes** the `{host, port, role}` entry — no manual cleanup; a later lookup misses and the front asks `ConnectionSupervisor` for a fresh one |
| front → `Connection` | **monitor** (not link) | on `:DOWN` the front fails every `pending` entry routed to that connection — one of the five terminal paths that clean an entry up, see Sequence; the crash never takes the front down. The front demonitors when it stops routing there |
| `MetadataStore` → its `:metadata` `Connection` | **monitor** | a mid-refresh `:DOWN` fails the in-flight refresh; the store re-acquires the connection on the next refresh |
| front → its `MetadataStore` | **monitor** (+ notify subscription) | a store restart makes ETS reads miss; the front parks callers in `:awaiting_metadata` and re-resolves once the store is `:ready` again |
| front → **the caller** | **monitor** per admitted request (the pid arrives free in `from`) | on `:DOWN` the front drops that `pending` entry and releases everything it held — most importantly its per-partition produce-gate slot, whose loss would stall that partition silently and permanently. A `GenServer.call` timeout kills an ordinary caller, so this covers the common give-up path; a caller that survives by catching the exit is covered instead by the entry's absolute deadline |
| front → its `Infra.Supervisor` | **link** (started in `init`) | this is the teardown mechanism: the front's death (any reason) tears the whole subtree down in order. Conversely, if `Infra.Supervisor` exhausts its restart intensity and exits, the non-trapping front dies with it and its parent recreates the client |
| `Infra.Supervisor` → {`Registry`, `ConnectionSupervisor`, `MetadataStore`} (`:rest_for_one`) | supervision link | a `MetadataStore` restart rebuilds from bootstrap (that front parks and re-resolves); a `Registry`/`ConnectionSupervisor` restart also restarts the store (its dependant) and drops connection registrations, recreated lazily |
| `KafkaEx.Supervisor` → per-client subtrees | supervision link | standard supervision; each client's front and its own infrastructure restart independently of every other client |

So the connection lifecycle is bookkept **twice, both automatically**: the `Registry` drops a dead
`Connection` from *routing* (lookups miss), and the front's `monitor` drops it from *in-flight
accounting* (`pending` entries fail). Neither is a hand-maintained map. The front holds exactly one
**link** — to its `Infra.Supervisor` — and everything else to individual infra processes is a
**monitor**: the link carries lifetime (teardown), while a `Connection` or `MetadataStore` crash
reaches the front only as a monitor `:DOWN` that leaves it alive to re-resolve. **Teardown is trivial
under per-client infrastructure:** the subtree has exactly one client, so the front's death tears it
down through that single link — no ref-counting of fronts and no idle-expiry negotiation (the question
that per-cluster sharing would have forced).

**Transport seam and testing.** The event-driven `Connection` replaces the synchronous
`NetworkClient.send_sync_request/3` Mimic seam that the current unit suite stubs. We introduce a
`Transport` behaviour (aligning with 06's `Transport.request/3` seam): production uses real
`gen_tcp`/`ssl`; **unit tests use an in-process fake transport** that delivers the *same* hand-built
wire bytes tests build today (e.g. `build_v0_metadata_response`) as `{:tcp, sock, data}` messages
instead of a function return — a mechanical port of the existing stubs. The decomposition is itself a
testability win: the front's state machine is testable against a **fake `Connection`** (a stub
replying `{:done, ref, result}`), and the `Connection` `:gen_statem` is testable against a **fake
`Transport`**. A handful of fake-broker tests (real loopback socket) exercise real framing and
multiplex/ordering; the existing Docker 3-broker integration suite remains the end-to-end gate.

### Startup

Two levels, top to bottom. The **OTP application boot is unchanged**; only what a client's `init`
does internally changes. Because infrastructure is per-client, **every** client boots its own
`MetadataStore` + connection set the same way — there is no "attach to an existing shared subtree"
path.

**1. Application boot (unchanged).** `KafkaEx.start/2` starts `KafkaEx.Supervisor` (a
`DynamicSupervisor`, `:one_for_one`) and, unless `disable_default_worker` is set, starts the default
worker (`kafka_ex.ex:176-194`). `disable_default_worker: true` — the common test / embedding setting —
boots the supervisor **empty**: no client, no per-client infrastructure, no `MetadataStore`, no
`Connection`, no socket to Kafka, until something explicitly starts a client. `mix test.unit` relies
on this: tests either start no client and drive modules directly, or start a client against the
in-process **fake `Transport`**.

```mermaid
sequenceDiagram
    participant App as KafkaEx.start
    participant Sup as KafkaEx.Supervisor
    participant Front as KafkaEx.Client front
    App->>Sup: start DynamicSupervisor
    alt disable_default_worker = true (tests / embedding)
        Note over Sup: boots empty - nothing connects to Kafka
    else default worker enabled (or client started explicitly)
        Sup->>Front: start_link default worker
    end
```

**2. Client boot.** The front's `init` stays **synchronous and fail-fast** (matching
`client.ex:95-181`): it starts and links its own `Infra.Supervisor` (`Registry`,
`ConnectionSupervisor`, `MetadataStore`); the store obtains a `:metadata` `Connection` **through the
`ConnectionSupervisor`**, negotiates ApiVersions, and performs the first metadata fetch into the ETS
snapshot. `init` **raises on any
failure** — a fail-fast infra child makes `Supervisor.start_link` return `{:error, …}` cleanly,
*without* a race and *without* the front needing to trap exits — so `start_link` returning means the
client is ready, and an unreachable cluster at boot crashes for the parent to retry. `initial_topics`
warms this first fetch; it does **not** force data connections.

```mermaid
sequenceDiagram
    participant Front as KafkaEx.Client front (init)
    participant Sup as KafkaEx.Supervisor
    participant Store as MetadataStore (this client's own)
    participant Meta as Connection meta/bootstrap
    Front->>Sup: start this client's own infrastructure subtree
    Note over Sup: one subtree per client - never shared
    Sup->>Store: start
    Store->>Meta: get :metadata Connection (via ConnectionSupervisor)
    Meta->>Meta: connect + negotiate ApiVersions
    Front->>Store: request first metadata (initial_topics)
    Store->>Meta: Metadata fetch
    Meta-->>Store: metadata -> atomic ETS snapshot
    Store-->>Front: ready
    Note over Front: raises on failure - else start_link returns ready
```

Every additional client (e.g. a group's `Manager` and each `GenConsumer`, absent a shared `:client`)
repeats **Client boot** independently: its own `MetadataStore`, its own negotiation, its own metadata
fetch, its own connection set. This is the deliberate cost of per-client infrastructure — the "N
clients per group" footprint is collapsed only when the caller opts into a shared `:client`
(`resolve_client`, #581), exactly as today. Automatic cross-client sharing is a future option (see
Alternatives).

**When connections are established.** Eager, at boot: only the meta/bootstrap `Connection` needed for
fail-fast. Lazy, on demand: per-broker **data-role `Connection`s** are created on the first request
routed to that `(node, role)`, via the `ConnectionSupervisor`. This is a deliberate improvement over
today, where `init` eagerly opens a socket to every bootstrap broker and `update_metadata` opens one
to every broker in metadata — including brokers the client never talks to (`client.ex:112-124`). Lazy
per-broker connect matches brod/Java/librdkafka.

### State machines

Two processes in the design are explicit `:gen_statem`s — the `Connection` and the `MetadataStore` —
because each has real, named lifecycle states; everything else (the front) stays a plain `GenServer`.

**`Connection`** mirrors librdkafka's per-broker state machine; states model real protocol steps:

```mermaid
stateDiagram-v2
    [*] --> connecting
    connecting --> authenticating: TCP up (SASL configured)
    connecting --> negotiating: TCP up (no auth)
    authenticating --> negotiating: SASL handshake ok
    negotiating --> connected: ApiVersions negotiated
    connected --> reconnecting: transport error / disconnect
    connecting --> reconnecting: connect times out (bounded window)
    reconnecting --> connecting: backoff elapsed — retry
    connected --> [*]: hard failure (crash - supervised)
    note right of connecting
        active connect, bounded window:
        requests postpone-d until connected
    end note
    note right of reconnecting
        backoff-wait during an outage — answers at once,
        never postpones. Atom keyed on whether the
        bytes were written: never written -> :not_connected
        (front parks, no attempt spent); written but
        unanswered -> :timeout (a real attempt)
    end note
    note right of connected
        {active, once} event-driven recv
        correlation_id to caller map
    end note
```

- `postpone` defers a request only **during an active connect, within a bounded window** (the
  `:connecting` state) — no hand-rolled queueing for the sub-second handshake. Once a drop pushes the
  connection into **`:reconnecting`** (backoff-wait during an outage) it does **not** postpone: it
  answers immediately, so its mailbox cannot grow unbounded. Waiting, where waiting is the
  right answer, happens in the **front** — the only process that holds the caller's absolute deadline.
  The `Connection` never learns that deadline, which is exactly why it must not be the process that
  parks on it. Symmetric with the store's `:degraded` mode.
- **The error atom is keyed on whether the request's bytes reached the socket.** That is the only
  thing separating "the broker never saw this" from "the broker may already have executed it", and the
  `Connection` is the sole process that knows which:
  - **never written** (it arrived while reconnecting) → `{:error, :not_connected}`. Re-sending cannot
    duplicate anything, so the front **parks** the entry until the `Connection` reports `:connected`
    or the caller's deadline expires — and **spends no retry attempt** on it.
  - **written, connection died before a response** → `{:error, :timeout}`, the same atom today's
    recv-deadline produces. `Retry.transport_timeout?/1` therefore keeps firing and coordinator
    requests keep their send-once rule (`client.ex:967-971`) bit-for-bit.
- Collapsing both into one retryable atom would do two silent kinds of damage: it would convert
  send-once coordinator requests into retried ones during precisely the rebalance/outage window where
  today's client deliberately refuses, and it would let three fail-fast refusals burn a caller's entire
  retry budget in microseconds without one real network attempt. Today's client is safe from the
  second only by accident — its retry recursion (`client.ex:929-956`) has **no** inter-attempt delay
  and is spaced solely by blocking I/O. Fail-fast removes that spacing, so it is replaced deliberately:
  by parking against a deadline rather than by counting attempts. Any delay in the front is a timer
  plus a parked entry, **never** `Process.sleep` — `Retry.with_retry/2` sleeps (`retry.ex:180`) and is
  therefore unusable in a process that serves every caller.
- On a transient disconnect it fails its in-flight requests (replying to the front under the split
  above), transitions to `:reconnecting` (backoff), and self-heals; hard/unexpected failures crash and
  are handled by supervision. It never re-implements a supervisor.
- Registered in the connection `Registry` keyed by `{host, port, role}` within its client's subtree,
  where `role ∈ {:data, :coordinator, :metadata}`. The **coordinator** gets its own connection —
  separate from data traffic even to the same physical broker — a convergent pattern in **all three**
  reference clients (see Prior art), and what removes the Heartbeat-vs-fetch head-of-line coupling
  behind the black-holed-broker stall. The dedicated **`:metadata`** socket is **brod-only** among the three; we adopt it
  anyway because sockets are cheap on the BEAM and it decouples metadata correctness from the
  fetch/data model.
- Multiple in-flight requests per socket are matched by a **`correlation_id → caller` map** (as brod
  and librdkafka do; order-independent). Per-partition **produce** ordering is enforced **in the front**
  (the router/retrier), *not* in the `Connection`: the front admits at most **N in-flight produce per
  partition** — `N = 1` today (a mute), `N ≤ 5` later gated by idempotent sequence numbers — so the
  connection never serializes across partitions and a front-driven retry can never reorder a partition.

**`MetadataStore`** is a `:gen_statem` whose three states are *lifecycle*, **not**
refresh-bookkeeping: `:loading` (no usable snapshot yet), `:ready` (serving, cluster reachable), and
`:degraded` (serving a stale snapshot, cluster currently unreachable). Refresh and `FindCoordinator`
are **data, not states** — a per-key `in_flight` map (see below). Reads never enter any state; they
hit ETS lock-free:

```mermaid
stateDiagram-v2
    [*] --> loading
    loading --> ready: first snapshot in ETS (metadata connection healthy)
    loading --> [*]: cold-boot fetch fails — fail-fast (front init raises)
    ready --> degraded: :metadata Connection down / refreshes failing
    degraded --> ready: connection recovered + snapshot refreshed
    note right of loading
        no usable snapshot yet -> reads miss, callers park.
        a store crash destroys the ETS table, so a
        restart re-enters loading and rebuilds
    end note
    note right of ready
        lock-free ETS reads never touch this process.
        refresh + FindCoordinator are DATA: a per-key in_flight
        map (per topic; per (coordinator_type, key)), coalesced,
        multiplexed over the :metadata connection
    end note
    note right of degraded
        still serves the stale snapshot; refreshes fail fast
        or park (bounded); emits degraded / recovered telemetry
    end note
```

- `:loading` obtains the store's `:metadata` `Connection` (through the `ConnectionSupervisor`),
  negotiates, and does the first metadata fetch into ETS. A failed first fetch is fail-fast (crashes
  the store; on cold boot the front's `init` raises). A **store crash destroys its ETS table**, so a
  restart also re-enters `:loading` and rebuilds — during that gap the front's reads miss and callers
  park until `:ready`.
- `:ready` — a fresh snapshot is in ETS and the cluster is reachable; fronts read lock-free, off the
  mailbox; refreshes complete normally.
- `:degraded` — the `:metadata` `Connection` is down (that `Connection`'s own `:gen_statem` is already
  retrying with backoff one level below); the store keeps serving the **stale** ETS snapshot, but a
  refresh it cannot complete either fails fast with `{:error, :no_broker}` — the atom today's client
  already returns when no broker can be selected (`client.ex:1361`) and which
  `Retry.transient_error?/1` already classifies as retryable — or parks within a
  bound (the store-side of the same "never park unboundedly during an outage" rule). A *new* atom
  here would be worse than a less descriptive one:
  unknown atoms fall through `Retry.transient_error?/1` to `false`, so naming the most transient
  condition there is would make the client give up on it permanently. The descriptive signal belongs
  in telemetry, not in the return value. Entering/leaving emits
  `[:kafka_ex, :metadata_store, :degraded]` / `:recovered` — a resilience signal aligned with the
  black-holed-broker motivation.
- **Refresh & discovery are data.** A per-key `in_flight` map holds one coalescing entry per refresh
  target (per topic) and per `(coordinator_type, key)` coordinator discovery, each with its waiter set
  and `epoch`. Concurrent triggers for the same key **join** the in-flight entry (this closes the #445
  race and de-storms coordinator re-discovery when a coordinator moves); different keys proceed in
  parallel, multiplexed over the one `:metadata` connection. On completion the store swaps the ETS
  snapshot atomically, bumps the `epoch`, and wakes its front. This is deliberately **not** a
  `:refreshing` state — one global refresh state would force global single-flight and serialize
  unrelated refreshes.
- The **lost-wakeup** guard is also data: each round carries an `epoch`, a parked front re-checks ETS
  after parking, and the store always sends a wakeup — so a front parking just after a refresh
  completes cannot miss it.

(`FindCoordinator` in-flight coalescing keyed per `(coordinator_type, key)` goes one step beyond
brod/Java/librdkafka — they coalesce only per-instance or dedup the *resolved* result, not the
in-flight lookup — justified here because the store owns discovery and a waiter list is cheap on the
BEAM.)

## Design decisions (resolved review)

Grilled one branch at a time; the connection-model, metadata-ownership, in-flight and coordinator
findings were cross-checked against **brod, the Java client, and librdkafka** (3/3 unless noted).

| # | Decision | Evidence / rationale |
|---|----------|----------------------|
| 1 | **Retry in the front; `Connection` is a dumb pipe** (one attempt → reply to front) | 3/3: all put retry above the transport |
| 2 | **`Connection` keyed per (node, role)** — coordinator/metadata separate from data | 3/3: dedicated coordinator connection; removes the black-holed-broker stall architecturally |
| 3 | **One `MetadataStore` per client** (`:gen_statem`; states `:loading`/`:ready`/`:degraded`), lock-free ETS reads, single-writer, per-key coalesced refresh + FindCoordinator, notify | one coalesced owner decoupled from connections; none uses a blocking read path. Per-client (not shared): Java/librdkafka are per-instance |
| 4 | **Front request-lifecycle state machine** (`:resolving/:in_flight/:awaiting_*`); coordinator discovery owned by the store | preserves today's retry rules incl. coordinator send-once |
| 5 | **Synchronous, fail-fast boot** (front ensures its own infrastructure subtree, then uses it) | preserves start/supervision contract |
| 6 | **Front = `GenServer`; `Connection` = `:gen_statem`** (self-healing; `postpone`) | per-request state ⇒ map, not process FSM; connection has real protocol states (librdkafka parallel) |
| 7 | **Per-client infra (`MetadataStore` + connections), supervised independently of the front but living/dying with the client** | front can't be a supervisor (caller holds its pid); no cross-client sharing ⇒ no teardown/ref-counting question |
| 8 | **`Transport` behaviour + in-process fake transport for unit tests** (+ some fake-broker + integration) | forced by `{active, once}`; mechanical port of existing stubs |
| 9 | **Multi-in-flight per connection matched by `correlation_id` map; ≤ 1 in-flight per partition for produce ordering** | 3/3 multiplex; Java mutes / idempotent-gates, brod `partition_onwire_limit`, librdkafka clamps under idempotence |
| 10 | **One multiplexed connection per (node, role), shared — not a connection pool** | 3/3: Kafka multiplexes over one TCP via `correlation_id` and orders per-connection; a pool multiplies FDs / broker-side conns for no throughput gain and breaks produce ordering |
| 11 | **No cross-client sharing of metadata/connections** (deferred future option, keyed by `{sorted bootstrap uris, ssl_options, auth}`) | brod shares only via an explicitly named client; auto-sharing adds teardown + blast-radius cost for a footprint already collapsible via a shared `:client` |
| 12 | **Front owns a linked `Infra.Supervisor` started in `init`, non-trapping; teardown via the link; sockets closed in each `Connection`'s `terminate/2`** | brod_client is the closest precedent (a worker owning its sub-supervisors); verified empirically that the parent-link tears the subtree down on any exit reason (incl. `:normal`) and that fail-fast `init` needs no `trap_exit` |
| 13 | **Per-partition produce-ordering gate lives in the front** (`max N in-flight per partition`, FIFO-parked): `N = 1` mute now, `N ≤ 5` idempotent-sequence gating deferred with the idempotent producer. A slot is held for the whole **logical** request — across retries, *not* per dispatch — and released on every terminal path (reply, retry exhaustion, deadline expiry, caller `:DOWN`, connection `:DOWN`), each release waking the next FIFO waiter | the gate and retry must co-locate, and retry is in the front (#1); 3/3 put the gate above the transport (brod `partition_onwire_limit`, Java mute, librdkafka toppar); releasing per dispatch would let a second producer interleave *between* attempts, defeating the ordering the gate exists for, and an unreleased slot silently stalls one partition forever |
| 14 | **In-flight requests keyed by a fresh `ref` per physical dispatch (staleness); a separate persistent `attempt` counter for backoff / max-retries / telemetry** | 3/3 decouple the two — brod `corr_id` vs `failures`, Java `correlationId` vs `ProducerBatch.attempts` (→ `record-retry-total`), librdkafka `rkbuf_corrid` vs `rkbuf_retries`; opposite lifetimes, so no single token does both |
| 15 | **Telemetry split by what a span measures: operation spans (`:produce`/`:fetch`/`:consumer.*`) stay in the front (same pid as today's client); the per-attempt `[:kafka_ex, :request]` span + `:connection`/`:auth`/`:connection.close` move to the `Connection`. Async forces manual `:start`/`:stop` (stored timestamps) instead of the synchronous `Telemetry.span/3` wrapper** | verified `client.ex:1383` — `[:kafka_ex, :request]` wraps one serialize→send→recv (bytes + broker), *not* the retried op; event names/measurements unchanged, only the transport events' emitting pid changes (front→Connection) → review tests asserting on emitter pid |
| 16 | **`MetadataStore` stays a `:gen_statem` with *lifecycle* states `:loading`/`:ready`/`:degraded` (NOT a `:refreshing` state); refresh/discovery is per-key `in_flight` data** | `:degraded` (cluster unreachable → serve stale + `degraded`/`recovered` telemetry + bounded refresh) is a genuine behavioural mode that earns the FSM; a `:refreshing` state would force global single-flight, conflicting with per-key coalescing (research: 3/3 model in-flight refresh as data, not a state) |
| 17 | **`FindCoordinator` discovery coalesced in-flight per `(coordinator_type, key)`, kept separate from metadata refresh** | de-storms concurrent re-discovery for one group (Manager + Heartbeat + commit at once); one step beyond brod/Java/librdkafka (which dedup only the resolved result or coalesce per-instance) — cheap on the BEAM (a waiter list) |
| 18 | **`Connection` `postpone`s only during an active connect (bounded window, `:connecting`); outside it the error atom is keyed on whether the request's bytes reached the socket — never written → `{:error, :not_connected}` (front parks it, no attempt spent); written but unanswered → `{:error, :timeout}` (a real attempt; coordinator send-once applies)** | bounds mailbox growth without moving the caller's deadline into the `Connection`, which never learns it; the split preserves today's send-once rule, whose reason is that the broker may already have executed a *written* request — a distinction one atom cannot carry; matches Java (`client.ready` gate + connect timeout) / librdkafka (outbuf message-timeout) |
| 19 | **`MetadataStore` is the sole owner of its `:protected`, single-writer ETS table; on a store crash the table is discarded and the restarted store rebuilds it** (reads briefly miss → front parks and re-resolves) | an ETS `heir` to preserve the table was considered and rejected as over-engineering: a BEAM table dies with its owner, so preserving it needs another live process holding it, and the brief rebuild park (one metadata fetch, one client) is cheaper than that machinery |
| 20 | **Rollback is `request_concurrency: :multiplexed` (default) \| `:serial`, implemented as a cap of one simultaneously admitted `pending` entry — not as a second request path**; temporary, removed in 1.3.0 | one entry at a time makes mailbox order equal execution order, restoring today's global total order exactly, without keeping two retry implementations alive in one release; a `:legacy` *code path* would be a third configuration nobody runs in production, and after PRs 1–5 there is no old path left to select anyway. Default `:multiplexed` because a safe default means the new path goes unexercised until the flag is removed — deferring risk, not reducing it |

## Delivery

Ships **incrementally** as reviewable, individually releasable PRs. The organising rule is that
**PRs 1–5 move responsibilities between processes without changing observable behaviour, and PR 6 is
the single semantic cutover.** Everything a reviewer must scrutinise for behaviour change, and
everything a rollback would have to undo, is therefore concentrated in one place instead of smeared
across the series. The public API's **shape** is unchanged throughout, so no step is source-breaking;
the one observable behavioural change — the loss of the global cross-caller request ordering that
today's single mailbox imposes — is stated in the compatibility contract and lands with PR 6.

| # | PR | What moves | Observable change |
|---|---|---|---|
| 1 | **Pure `Broker` + `Transport` seam** | The socket leaves the `Broker` struct, which becomes a plain value; the client holds a separate `node_id ⇒ connection` map; a `Transport` behaviour wraps `NetworkClient`, with an in-process fake for tests | None. Metadata refresh stops transplanting live sockets, which is a latent-bug fix, not a contract change |
| 2 | **Selection + coordinator cache into `ClusterMetadata`** | Leader/controller/coordinator selection and refresh-on-error move out of `Client` (today `client.ex:1061-1174`) behind a small interface | None; still in-process and synchronous |
| 3 | **`Connection` as `:gen_statem`** | One connection per `{host, port, role}` under a `ConnectionSupervisor` + `Registry`, both owned by the client's `Infra.Supervisor`; sockets, SASL and ApiVersions negotiation move into it. **The client still calls it synchronously** | None by contract. The largest single step and the first candidate to split further |
| 4 | **`MetadataStore` as `:gen_statem`** | ETS table, the store's own `:metadata` connection, per-key coalesced refresh and `FindCoordinator`; the client reads ETS lock-free instead of holding `cluster_metadata` in its state. Request path still synchronous | Closes #445 on its own |
| 5 | **Telemetry split + deadline threading** | Per-attempt `[:kafka_ex, :request]` and the `:connection`/`:auth` events move to the `Connection`; operation spans stay in the front. Separately, `KafkaEx.API` threads the caller budget it already computes into the request message | Emitting pid changes for transport events; shapes identical. The threaded deadline is unused until PR 6, which is what keeps that PR small |
| 6 | **Cutover: the non-blocking front** | `handle_call` returns `{:noreply, …}`; the `ref`-keyed `pending` map, absolute deadlines, caller monitors, the per-partition produce gate, and the retry state machine | **The one semantic step.** Order becomes per-caller rather than global; head-of-line blocking (#357) and the black-holed-broker heartbeat stall are removed here |
| 7 | **Cleanup** | Delete the recursive synchronous retry loop and the now-dead `send_sync_request` paths | None |

Ordering note: the `Connection` (3) deliberately precedes the `MetadataStore` (4), even though the
store closes #445 and would deliver visible value sooner. The store owns its own `:metadata`
connection, so landing it first would mean giving it a connection in the old shape and then rebuilding
it in step 3 — paying for the same work twice.

### Rollback

A minor bump carrying a concurrency change needs an escape hatch, and we have our own precedent for
why: v1.1.0's metadata-refresh behaviour change required the unplanned v1.1.1 patch cycle three days
later, and two entries in that changelog are marked "Behavior change". PR 6's failure mode is worse in
kind — races, the class that survives CI and appears under production load.

The escape hatch is **`request_concurrency: :multiplexed` (default) | `:serial`**, and the important
part is what it is *not*: it does **not** select between two request paths. `:serial` caps the number
of simultaneously admitted `pending` entries at **one**. Same state machine, same retry
classification, same telemetry — the front simply handles entries one at a time, so mailbox order
becomes execution order again and today's global total order is restored exactly. There is no second
retry implementation to keep alive, which matters because retry is where the correctness lives.

Two things about it must stay explicit:

- **It restores head-of-line blocking on purpose.** A user choosing `:serial` is knowingly trading
  back the #357 and black-holed-broker fixes for the old ordering. That is the correct trade for a rollback switch
  and a bad one for a default.
- **It does not roll back the process topology.** Sockets still live in `Connection`s and metadata in
  the `MetadataStore`, so a defect introduced by PR 3 or PR 4 is *not* recoverable with this flag; for
  those, the rollback is a version pin to 1.1.x. The flag covers the cutover, nothing else.

The default is `:multiplexed` from 1.2.0 rather than a cautious `:serial`. A safe default sounds
prudent but means nobody exercises the new path, so its defects surface only when the flag is removed
— deferring the risk instead of reducing it, while withholding the fixes that motivate the work.
`:serial` is documented from the outset as a temporary hatch, slated for removal in 1.3.0.

### Test strategy

The existing suite carries a **quantified migration cost, and it falls due at PR 3, not PR 6**. Eight
test files make **26 direct `handle_call/3` calls**, asserting on the synchronous return value. A
comment left by an earlier maintainer in `test/kafka_ex/client/transport_error_test.exs:27-29` already
anticipates this refactor — "the request runs in THIS process … a future refactor to a real GenServer
would need `set_mimic_global`" — but understates it: after the cutover `handle_call/3` returns
`{:noreply, state}`, so there is no value left to assert on. These are rewrites against the async
protocol, not a change of mock mode. And they come due at PR 3, because that is where I/O first
leaves the test process: Mimic's private mode binds stubs to the *calling* process, so stubs set in a
test stop applying the moment the socket lives in a `Connection`.

- **The answer is the injected fake, not `set_mimic_global`.** Decision #8's `Transport` behaviour and
  the fake `Connection` are passed in as configuration, so tests stay `async: true` and deterministic.
  Global Mimic mode would force `async: false` across the client suite and reintroduce cross-test
  interference — curing the symptom at the cost of the property that makes the suite trustworthy. The
  ten `NetworkClient` stub sites migrate onto the fake.
- **CI's automatic retries must be off for PRs touching the front.** `integration-tests.yml` (four
  jobs) and `chaos-tests.yml` each retry failures twice. That masks precisely the failure class this
  refactor introduces: a genuine race looks like a flake and passes on the second attempt. For the
  duration of the cutover a retry is evidence, not noise.
- **Model-based coverage of the front's state machine** (**proposed — needs maintainer sign-off, as it
  adds `{:stream_data, "~> 1.1", only: [:dev, :test]}`**). The invariants at risk are quantified over
  *interleavings*, so example tests can only sample them: every admitted entry reaches **exactly one**
  terminal path; no gate slot is ever leaked; the retry budget never goes negative; no entry replies
  twice. A leaked gate slot stalls one partition silently and forever, which is exactly what retried
  CI hides. **This is worth doing only if the front's decision logic is a pure
  `transition(entry, event) → {entry', effects}` function** — then the property test needs no
  processes, no sockets and no clock, runs fast, and shrinks to a readable counterexample. If that
  logic is instead spread across `handle_info` clauses, a property test would have to drive a live
  process against wall-clock time, would be slow and flaky, and should not be written; stay with
  example tests in that case. The purity requirement is worth adopting on its own merits: it keeps
  every retry decision in one readable place.

### Benchmarks

There is no benchmark infrastructure today — no `bench/` directory and no `benchee` dependency — so a
baseline has to be created before anything moves. A small suite lands in **PR 1**, precisely so the
baseline exists while the client is still the code we ship, and is re-run after PRs 3, 4 and 6.

Two costs this architecture introduces are worth measuring rather than asserting away:

- **A cross-process hop per request.** Two extra message sends in each direction. Cheap on the BEAM,
  not free.
- **An ETS read per request copies the term out of the table.** Today metadata lives in the client's
  own process state, so a leader lookup is a map read with no copy. This is not an argument against
  ETS; it is an argument for a **narrow** read, and we adopt that as a design constraint: the hot-path
  lookup is keyed `{topic, partition}` and returns the minimal routing tuple. Reading a whole topic's
  partition structure per request would be a copy that does not exist today.

Measure produce and fetch throughput and p99 latency at **1, 10 and 100 concurrent callers**. The two
ends carry different burdens of proof: the **single-caller** case is the one at risk — it pays the hop
and the copy and gains nothing from multiplexing — while the **100-caller** case is where the win must
appear, since without it the work has no justification. The acceptance threshold for single-caller
regression is deliberately left for the maintainers to set once PR 1's baseline exists; naming a
percentage now would be inventing one.

`benchee` (dev/test only) is proposed alongside `stream_data`; both are **subject to maintainer
sign-off**, as this RFC does not unilaterally add dependencies.

### Success criteria

The work is done, and this RFC is discharged, when all of the following hold:

1. **#357 is closed by construction**: a long-poll `fetch` at the log end no longer delays any other
   caller's request. Demonstrated by a test that issues a `fetch` with a long `wait_time` and asserts
   an unrelated `metadata` call returns promptly on the same client.
2. **The black-holed-broker stall cannot occur**: a broker accepting TCP but never replying stops
   affecting requests to other brokers, and a consumer-group `Heartbeat` is served throughout.
   Demonstrated in the chaos suite, which already has the fault injection for it.
3. **#445 is closed**: metadata reads no longer queue behind socket I/O, and a produce cannot observe
   the stale mismatched metadata that race produced.
4. **No behavioural change beyond the one declared.** The compatibility contract holds item by item:
   same public API shape, same process identity, same telemetry event shapes, same error atoms — with
   per-caller rather than global ordering as the single declared exception.
5. **No unexplained performance regression.** Single-caller p99 latency stays within the threshold the
   maintainers set against PR 1's baseline, and the 100-caller case shows the throughput improvement
   that justifies the work. A regression at high concurrency invalidates the premise and is a blocker,
   not a tuning task.
6. **`request_concurrency: :serial` reproduces today's behaviour** on the same test suite, so the
   rollback path is proven rather than assumed.

## Drawbacks

- Converting the recursive, blocking retry loop into the asynchronous `ref`-keyed state machine is
  the main correctness risk (state can interleave across concurrent calls).
- The `Connection` `:gen_statem` (SASL/ApiVersions handshake, self-healing reconnect) is more
  machinery than a plain socket call.
- **Per-client infra multiplies the resource footprint**: N clients to the same cluster keep N
  `MetadataStore`s, N metadata-refresh loops, and N connection sets — the "N clients per group" waste
  is *not* removed here (it stays collapsible only via a shared `:client`). We accept this because it
  keeps the trust boundary trivial and the blast radius per-client, and because cross-client sharing —
  with its teardown/ref-counting and wider blast radius — can be added later as an opt-in without a
  breaking change.
- Async multiplexing must preserve per-partition produce ordering (≤ 1 in-flight per partition, or
  idempotent sequence numbers) and re-key `correlation_id` per connection.
- The transport events (`[:kafka_ex, :request]`, `:connection`, `:auth`) move to the `Connection`, so
  their emitting pid changes (operation spans stay in the front, pid unchanged); event shapes are
  identical, but emission shifts from the synchronous `Telemetry.span/3` wrapper to manual
  `:start`/`:stop` under the async model — tests asserting on the emitting pid must be reviewed.
- It moves KafkaEx away from its deliberately centralized design; we bound this by stopping at
  per-broker/per-role connections and a per-client store (see alternatives).

## Rationale and alternatives

- **Keep the monolith; tune timeouts only.** Does not remove head-of-line blocking; the v1.1.0 patch
  cycle showed timeout tuning only shifts the failure window.
- **Full process-per-topic-partition data plane** (brod-style `brod_producer`/`brod_consumer`).
  Rejected: reference clients and our review lenses flag it as over-engineering for KafkaEx's goals;
  it fights the centralized design and multiplies supervision complexity. We borrow the **per-broker
  (× role)** split, not the per-partition one.
- **Shared per-cluster metadata/connections (keyed by security identity).** Rejected as the default,
  kept as a future opt-in: it would remove the N-clients-per-group waste, but at the cost of
  cross-client blast radius, a subtree-teardown /
  ref-counting question, and a trust boundary that must be enforced on every socket. Reference clients
  are per-instance (Java, librdkafka) or share only through an explicitly named client (brod); the same
  footprint is already collapsible here via a shared `:client`.
- **A classic connection pool** (N interchangeable sockets per broker). Rejected: Kafka multiplexes
  over one connection and orders per-connection, so a pool adds cost and breaks produce ordering for
  no throughput gain; the throughput lever is the in-flight cap.
- **Front as `:gen_statem`.** Rejected: the front multiplexes many concurrent requests, so per-request
  state must live in a map, not in a single process state. The `Connection` is the correct FSM.
- **Chosen: control-plane / data-plane split with per-client infra.** Satisfies every driver
  (head-of-line blocking, #445, the black-holed-broker stall) and unifies the three earlier notes it supersedes — pure
  `Broker`, decentralised data plane, deepened `ClusterMetadata` — into one coherent target, replacing
  the fourth note's cluster-shared store with per-client infrastructure, while leaving sharing as a clean
  future opt-in.

## Security considerations

The change is close to neutral here, but not entirely, so the three points worth a reviewer's
attention:

- **The credential trust boundary stays trivial, and that is a deliberate consequence of per-client
  infrastructure.** Because no `Connection` is ever shared between clients, a socket authenticated
  with one client's SASL credentials can never carry another client's traffic. Cross-client sharing
  would have required enforcing that boundary on every socket — one of the reasons it is rejected as
  the default (see Alternatives).
- **SASL handshakes move into the `Connection` `:gen_statem`, and its `:authenticating` state must
  fail closed.** A connection that has not completed authentication is not `:connected`, so no
  request can be dispatched over it; the existing `:plain_requires_tls` enforcement moves with the
  handshake and is not weakened. Credentials must not appear in the `Connection`'s state dumps —
  `sys:get_state/1` and crash reports on a `:gen_statem` print state by default, which the current
  synchronous path does not expose in the same way.
- **The new telemetry emitters must not widen what is published.** The `:auth` events move to the
  `Connection`; their measurements and metadata stay as they are, and no credential material enters
  them.

No new network listener, no new port, no new configuration that accepts a secret, and no change to
how SSL options are resolved.

## Prior art

Source-grounded (read from current `master`/`trunk`), organised by the four axes the design turns on.

- **brod / kafka_protocol (Erlang).** `kpro_connection` is a `proc_lib` process per TCP connection,
  socket in `{active, once}`, pipelining multiple in-flight requests matched by a
  `correlation_id → caller` map (`kpro_sent_reqs`); it holds **zero** Kafka business logic. Retry
  lives above it in `brod_producer` (`is_retriable/1`, `schedule_retry`). Metadata + a connection
  registry (keyed by `{host,port}`) live in `brod_client`, with a **dedicated `meta_conn`** for
  metadata/coordinator; refresh is lazy, single-flighted by the mailbox. The group coordinator gets
  its **own dedicated connection** (explicit "connections in brod_client are shared … coordinator has
  to be dedicated"). Produce ordering: `partition_onwire_limit` default **1**.
- **Apache Kafka Java client.** `NetworkClient` + `Selector` is a single-threaded non-blocking loop
  (`poll()`), not thread-per-connection. `InFlightRequests` tracks a per-node `Deque` (FIFO;
  `correlation_id` for sanity only), `max.in.flight.requests.per.connection` default **5**. A single
  shared `Metadata` object single-flights its refresh (`needFullUpdate`/`needPartialUpdate` flags + a
  version counter); `metadata.max.age.ms` (periodic) + `metadata.max.idle.ms` (idle-topic eviction). Retry lives in
  `Sender`/coordinator (`InvalidMetadataException → metadata.requestUpdate`), never in
  `NetworkClient`. The coordinator is a separate logical connection (`GroupCoordinatorNode`, id
  prefixed `+`) to the same physical broker. Ordering: mute the partition at in-flight 1, or
  idempotent sequence-number gating up to 5.
- **librdkafka (C).** One thread per broker (`rd_kafka_broker_thread_main`, `rd_kafka_broker_t`) owning
  its socket; multiple in-flight matched by `correlation_id` in `rkb_waitresps`; `max.in.flight`
  default **1,000,000** (clamped to 5 with idempotence). A **global** `rk_metadata_cache` (per client,
  not per connection) with dedup via `rd_kafka_metadata_cache_hint` (skips a refresh already in
  flight); `topic.metadata.refresh.interval.ms` 300 000, `metadata.max.age.ms` 900 000. Retry/refresh
  decisions are in `rdkafka_request.c` (toppar-aware `RD_KAFKA_ERR_ACTION_REFRESH/RETRY`), transport
  is dumb. The coordinator is its **own logical broker + thread** (`rd_kafka_broker_add_logical`),
  separate from the physical broker's data connection.

**Mapping to this RFC:** (a) retry above the transport — 3/3; (b) a metadata owner decoupled from
connections — 3/3 (single-flighted in Java/librdkafka; brod owns it separately but re-fetches);
(c) multi-in-flight per connection — 3/3; (d) coordinator on its own connection — 3/3. None fuse socket I/O and metadata in one mailbox as KafkaEx does today.

## Unresolved questions

Every design branch raised in the review is now resolved and captured in **Design decisions** above
(#1–#20). What remains is implementation-level and out of scope for this RFC: exact timer/backoff
constants (connect window, reconnect backoff, `:degraded` bound, retry budget), the precise
`:degraded` refresh policy (fail-fast vs bounded park), and the ETS snapshot representation. The
**Forward compatibility** and **Future possibilities** sections below list capabilities deliberately
deferred.

Two questions are genuinely open and need a maintainer answer before implementation starts:

- **Two dev-only dependencies** — `stream_data` for model-based coverage of the front's state machine
  and `benchee` for the baseline. Both are argued for under Test strategy and Benchmarks; neither is
  added unilaterally by this RFC.
- **The single-caller latency threshold**, which can only be set once PR 1 establishes a baseline.

## Forward compatibility — does this foreclose anything?

Three capabilities we do not build here but must not paint ourselves out of. For each: is it
possible, does this design *block* it, what is already present, what stays additive, and the **one
constraint** to respect so we keep the door open.

### Transactional / idempotent producers

**Possible, and this design *enables* rather than blocks it.** The wire layer is already in place:
`transactional_id` is carried on Produce V3–V8, and `KafkaEx.API.find_coordinator/3` **already**
accepts `coordinator_type: :group | :transaction` (`api.ex:723-737`, `messages/find_coordinator.ex`).
What is missing is purely client-side session logic: `InitProducerId`, `AddPartitionsToTxn`,
`AddOffsetsToTxn` / `TxnOffsetCommit`, `EndTxn`, and a per-`(producer_id, partition)` sequence counter
in the produce path (today only `messages/fetch.ex` reads `producer_id`).

This architecture is the right substrate for three reasons: (a) the **coordinator role** generalizes
directly — the transaction coordinator is just `coordinator_type: :transaction` on the same dedicated
`:coordinator` connection and the same store-owned `FindCoordinator`; (b) **decision #9** (≤ 1
in-flight per partition, or idempotent sequence-number gating) is exactly what idempotent/transactional
produce requires (strict per-partition order, bounded in-flight, no gaps); (c) **retry lives in the
front** (decision #1), which is the one place re-sequencing on retry must happen. A single multiplexed
`:data` connection is fine — the broker tracks sequences per `producer_id`, so different producers'
writes may interleave on the wire.

**Constraint to keep the door open:** the transactional session (pid/epoch, per-partition sequence,
added-partitions set, commit/abort state, epoch fencing) is **producer-session control-plane state
with a single owner per `transactional.id`** and strict in-order, gap-free dispatch per partition. It
must layer **above** the `Connection`, not inside it — connections stay dumb pipes; sequence state
never scatters into them.

### Broadway / GenStage integration

**Possible, and this design *unblocks* it** (there is no Broadway/GenStage code today; the official
`BroadwayKafka` is `brod`-based, so this would be a new connector). A Broadway producer is a
**demand-driven (pull)** GenStage producer, whereas KafkaEx's consumer is **push**
(`GenConsumer.handle_message_set`); a connector would drive fetch itself on demand rather than through
`GenConsumer`. The blocker today is precisely **#357** — a blocking fetch stalls the whole client, so
a demand-driven pipeline stage would freeze. Removing head-of-line blocking (per-broker isolation) and
making heartbeats reliable under load (fixing the black-holed-broker stall) is exactly what a Broadway pipeline needs.

**Constraint to keep the door open:** KafkaEx has **no async request primitive today**, and this RFC
does not add one — `send_request/4` is, and stays, blocking and send-once (see the compatibility
contract). The constraint is therefore about *not foreclosing* one: the front's internal
`{:done, ref, result}` path must keep a shape into which a future **additive** public async API (a
call that returns a `ref` and later delivers `{:done, ref, result}` to the caller) can be layered
without rearchitecting the front. Without such a primitive a pull pipeline is capped at one in-flight
fetch per producer process. That future API is also the single place where the free-backpressure
property (one in-flight call per caller process) stops holding, so it — not this RFC — is where a
per-connection in-flight cap becomes necessary (decision #9).
Everything else stays additive — the `Broadway.Producer` (assignment, ack → offset commit,
drain-before-revoke) lives in the connector; KafkaEx itself need not become GenStage-aware.

### Distributed Erlang (one Kafka cluster, many BEAM nodes)

**Not blocked — the design is node-local by construction, which is the correct default.** All of a
client's infrastructure — the Elixir `Registry`, the metadata ETS table, the TCP sockets — is
node-local, **not** shared across the Erlang cluster. Each BEAM node connects to Kafka independently;
Kafka's own group coordinator handles membership across processes, nodes, and hosts. We deliberately
do **not** route Kafka traffic over Erlang distribution (serializing payloads over dist, coupling node
lifetimes, and head-of-line blocking on the dist channel is an anti-pattern). Addressing a front from
another node is unchanged: `:name` may still be `{:global, term}` (`api.ex:265-268`), and
`GenServer.call({:global, name}, …)` routes the request to the front's node, which uses *that* node's
local infra.

**Constraint to keep the door open:** register a client's infra (`MetadataStore`, connection
`Registry`, the per-client supervisor) with a **node-local** name/registry — **never `:global`**. A
global store/supervisor would concentrate every node's Kafka sockets onto one node and force all other
nodes' traffic across Erlang distribution: a bottleneck and a coupling. The front's own optional
`:global` `:name` is orthogonal and stays allowed. Should cross-client sharing ever be added, this
same node-local constraint applies to the shared subtree.

## Future possibilities

- Per-partition producer processes with send buffers (only if measurements justify it — deliberately
  out of scope here).
- A bounded, configurable number of `:data`-role connections per broker (a small pool) *only* if
  measurements ever show a single multiplexed socket is the bottleneck — and never splitting one
  partition's produces across sockets (would break ordering).
- Independent liveness detection (TCP keepalive / periodic probe) so the transport timeout is no
  longer the sole detector of a silently-hung broker.
- Partition-count-change awareness and cooperative rebalancing, which an actively-refreshed
  metadata store makes materially easier.

## References

- `lib/kafka_ex/client/client.ex`, `lib/kafka_ex/client/state.ex`,
  `lib/kafka_ex/network/network_client.ex`, `lib/kafka_ex/cluster/broker.ex`
- Issues #357, #445
