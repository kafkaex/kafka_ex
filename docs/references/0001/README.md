# RFC 0001 — mechanism references

Companions to [RFC 0001 — Non-blocking process architecture for `KafkaEx.Client`](../../rfcs/0001-client-process-architecture.md).

These documents describe **how** the proposed design works. They are non-normative: every decision,
and every trade-off a reviewer is asked to accept, lives in the RFC itself. Read them to check the
design is sound, or to implement it.

| Document | Answers |
|---|---|
| [Request lifecycle](request-lifecycle.md) | What a request's state looks like in flight, how the front resolves a target and classifies a result, and the five ways an entry ends |
| [Supervision, ownership and teardown](supervision-and-ownership.md) | Which process owns what, which edges are links and which are monitors, what a crash takes down, how a client tears down |
| [Startup and application boot](startup.md) | What `start_link/2` does, why boot is synchronous and fail-fast, why `disable_default_worker` still means no sockets |
| [State machines](state-machines.md) | The `Connection` and `MetadataStore` `:gen_statem`s — states, transitions, and what each state refuses to do |
